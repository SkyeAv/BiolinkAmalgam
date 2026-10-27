package biolinkamalgam

import biolinkamalgam.config.Config
import biolinkamalgam.export.{DatasetCard, Manifest, Parquet, Tsv}
import biolinkamalgam.source.Registry
import biolinkamalgam.transform.{CliqueMerge, Filters, TaxonInference, Uniqueness}
import org.apache.spark.sql.SparkSession

import java.util.concurrent.{Executors, TimeUnit}

/** Stage orchestration. Stages are data-dependent and run sequentially on purpose (ingest → merge → arbitrate); only
  * the four independent SINKS fan out concurrently off one persisted DataFrame — one computation, parallel writes.
  */
object Pipeline {

  case class Options(
      babelDir: Option[os.Path],
      customDir: Option[os.Path],
      outDir: os.Path,
      taxaConfig: os.Path,
      promoteConfig: os.Path,
      filtersConfig: os.Path,
      simpleExport: Boolean = false,
      datasetMeta: Map[String, String] = Map.empty,
      sample: Option[Long] = None,
      ingestCache: Option[os.Path] = None,
      gtdbDir: Option[os.Path] = None,
      audit: Boolean = false,
      clinvarFile: Option[os.Path] = None,
      oboDir: Option[os.Path] = None,
      oboConfig: Option[os.Path] = None,
      // Identifier-namespace taxon inference (config/curie_taxa.yaml); None disables it.
      curieTaxaConfig: Option[os.Path] = None
  )

  def run(spark: SparkSession, opts: Options): Seq[Manifest.Stage] = {
    os.makeDir.all(opts.outDir / os.up)
    val workspace = os.temp.dir(dir = opts.outDir / os.up, prefix = ".amalgam-stages-")
    try runStaged(spark, opts, workspace)
    finally os.remove.all(workspace)
  }

  private def runStaged(spark: SparkSession, opts: Options, workspace: os.Path): Seq[Manifest.Stage] = {
    def materialize(name: String, df: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
      val path = workspace / name
      df.write.parquet(path.toString)
      spark.read.parquet(path.toString)
    }
    val stages = scala.collection.mutable.ListBuffer.empty[Manifest.Stage]

    def timed[A](name: String)(body: => (A, Long)): A = {
      val start = System.nanoTime()
      val (result, rows) = body
      stages += Manifest.Stage(name, rows, (System.nanoTime() - start) / 1000000)
      result
    }

    val taxa = Config.loadTaxa(opts.taxaConfig)
    val promotePrefixes = Config.loadPromotePrefixes(opts.promoteConfig)
    val filters = Config.loadFilters(opts.filtersConfig)
    val inference = opts.curieTaxaConfig.map(TaxonInference.load).getOrElse(TaxonInference.Rules.empty)

    // [1] INGEST — union all registered sources into the canonical clique table,
    // then apply declarative category/prefix/taxon-allowlist filters. With
    // --ingest-cache, the result is materialized as parquet once; reruns that
    // keep the fingerprint (same snapshot files + same ingest-affecting config)
    // read it back in seconds instead of re-parsing 100GB+. Name-level gates
    // live in [3]+[4], so filters.yaml *gate* edits never invalidate the cache.
    val oboEntries = opts.oboDir.map { dir =>
      dir -> source.OboRegistry.entries(opts.oboConfig.getOrElse(os.pwd / "config" / "obo_sources.yaml"))
    }
    // Additive sources (ClinVar, OBO) are small and loaded fresh every run, so the multi-GB core ingest cache
    // (BABEL + custom + GTDB) keeps its fingerprint and stays a cache hit when they are added or updated.
    val extraSources = Registry.forConfig(None, None, None, promotePrefixes, opts.clinvarFile, oboEntries)
    val extraPrefixes =
      (opts.clinvarFile.toSeq.flatMap(_ => Seq("CLINVAR", "DBSNP")) ++ oboEntries.toSeq.flatMap(_._2.map(_.prefix)))

    val coreCliques = timed("ingest") {
      val sources = Registry.forConfig(opts.babelDir, opts.customDir, opts.gtdbDir, promotePrefixes)
      val fresh = () => Filters(Registry.loadAll(spark, sources, opts.sample), filters, taxa.allowlist)
      opts.ingestCache match {
        case None =>
          val df = fresh()
          (df, df.count())
        case Some(dir) =>
          val fp = fingerprint(opts, taxa, promotePrefixes, filters)
          val fpFile = dir / "_fingerprint"
          if (os.exists(fpFile) && os.read(fpFile) == fp) {
            System.err.println(s"[biolinkamalgam] ingest cache hit: $dir")
            val df = spark.read.parquet(dir.toString)
            (df, df.count())
          } else {
            System.err.println(s"[biolinkamalgam] ingest cache rebuild: $dir")
            fresh().write.mode(org.apache.spark.sql.SaveMode.Overwrite).parquet(dir.toString)
            os.write.over(fpFile, fp) // only after the write fully succeeded
            val df = spark.read.parquet(dir.toString)
            (df, df.count())
          }
      }
    }

    var extraCliques: Option[org.apache.spark.sql.DataFrame] = None
    val ingested =
      if (extraSources.isEmpty) coreCliques
      else
        timed("ingest_extra") {
          val extra = Filters(Registry.loadAll(spark, extraSources, opts.sample), filters, taxa.allowlist)
            .persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
          extraCliques = Some(extra)
          // Leaders are folded per source, so the union is only correct if no core leader uses an extra prefix.
          // A column-pruned prefix scan of the cache proves it in seconds instead of re-consolidating 140M rows.
          val clash = coreCliques
            .filter(
              org.apache.spark.sql.functions
                .split(org.apache.spark.sql.functions.col("leader"), ":")(0)
                .isin(extraPrefixes: _*)
            )
            .limit(1)
            .count()
          require(clash == 0, s"core sources already carry leaders with prefixes ${extraPrefixes.mkString(",")}")
          val df = coreCliques.unionByName(extra)
          (df, extra.count())
        }

    // [1b] TAXON INFERENCE: taxonless cliques take the taxa of single-species identifier namespaces, then the
    // allowlist is re-applied so they meet the same top-taxa cutoff as BABEL-tagged cliques. Runs after the
    // ingest cache on purpose: curie_taxa.yaml edits never invalidate the multi-GB cache.
    val cliques =
      if (inference.isEmpty) ingested
      else Filters.taxonAllowlisted(TaxonInference(ingested, inference), taxa.allowlist)

    // [2] MERGE CLIQUES — preferred-name string matching, scoped by (category, taxon).
    // Every DataFrame consumed by [5] is persisted: export runs long after these
    // stages, and Spark's ContextCleaner GCs their shuffle files — an unpersisted
    // side output would recompute from missing shuffle data (exit 56,
    // FileNotFoundException on shuffle_*.data, seen on the 2026jul22 full run).
    val auditLevel = org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK
    val (merged, merges) = timed("merge") {
      val result = CliqueMerge(cliques, taxa.majorTaxa, materialize)
      // A durable boundary releases merge shuffle dependencies before arbitration/export. The narrow mapping
      // and audit frame remain readable from workspace until the run completes, not from shuffle spill files.
      val merged = materialize("merged", result.cliques)
      (merged, result.merges) -> merged.count()
    }

    // [3]+[4] GATE (junk filters inside) + ARBITRATE — global synonym uniqueness.
    val (output, conflicts) = timed("arbitrate") {
      val result = Uniqueness(merged, taxa.majorTaxa, filters)
      result.output.persist(auditLevel)
      if (opts.audit) result.conflicts.persist(auditLevel)
      (result.output, result.conflicts) -> result.output.count()
    }

    // [5] EXPORT — the parquet deliverable always; conflicts/merges TSVs only with
    // --audit. At BABEL scale the full audit is hundreds of GB of lost synonyms —
    // nobody reviews that, and writing it starved the local executor of heartbeats
    // (exit 56 on the 2026jul22 run). The small-scale test suite still audits.
    timed("export") {
      val pool = Executors.newFixedThreadPool(4)
      try {
        val parquetFut =
          pool.submit[Unit](() =>
            Parquet.writeSingleFile(output, opts.outDir / "biolinkamalgam.parquet", opts.simpleExport)
          )
        val auditFuts =
          if (opts.audit)
            Seq(
              pool.submit[Unit](() => Tsv.writeSingleFile(conflicts, opts.outDir / "conflicts.tsv")),
              pool.submit[Unit](() => Tsv.writeSingleFile(merges, opts.outDir / "merges.tsv"))
            )
          else Seq.empty
        // Typed crossref edges, one parquet per predicate under crossrefs/ (one HF config each), so consumers
        // choose which edge types to conflate. Never merged here: they are associations, not equivalence.
        // Each file is replaced only once its new footer is complete; stale files are pruned after every write
        // succeeded, so a failed export never leaves the previous card pointing at deleted files.
        val crossrefDir = opts.outDir / "crossrefs"
        val edges = Registry
          .crossrefs(spark, extraSources, opts.sample)
          .map(_.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK))
        val predicates = edges.toSeq.flatMap(Registry.crossrefPredicates)
        val crossrefFuts =
          for (e <- edges.toSeq; p <- predicates)
            yield pool.submit[Unit](() =>
              Parquet.writeSingleFile(
                e.filter(org.apache.spark.sql.functions.col("predicate") === p).orderBy("subject", "object", "source"),
                crossrefDir / s"$p.parquet",
                opts.simpleExport
              )
            )
        parquetFut.get()
        auditFuts.foreach(_.get())
        crossrefFuts.foreach(_.get())
        edges.foreach(_.unpersist(blocking = false))
        val expected = predicates.map(p => s"$p.parquet").flatMap(f => Seq(f, s".$f.crc")).toSet
        if (predicates.isEmpty) os.remove.all(crossrefDir)
        else os.list(crossrefDir).filterNot(f => expected(f.last)).foreach(os.remove.all(_))
        os.remove(opts.outDir / "crossrefs.parquet", checkExists = false) // pre-split layout
        os.remove(opts.outDir / ".crossrefs.parquet.crc", checkExists = false)
        val sourceMeta = opts.datasetMeta ++ Map(
          "clinvar" -> opts.clinvarFile.map(_.toString).getOrElse("none"),
          "obo" -> opts.oboDir.map(_.toString).getOrElse("none"),
          // provenance: which identifier-namespace mapping produced the run (first 12 hex of its digest)
          "taxon_inference" -> (if (inference.isEmpty) "none"
                                else s"${inference.fingerprint.hashCode.toLong.toHexString}"),
          "taxon_allowlist" -> taxa.allowlist.size.toString
        )
        DatasetCard.write(opts.outDir, "biolinkamalgam.parquet", stages.toSeq, sourceMeta, predicates)
        () -> output.count()
      } finally {
        pool.shutdown()
        pool.awaitTermination(1, TimeUnit.MINUTES)
      }
    }

    Seq(merged, output, merges, conflicts).foreach(_.unpersist(blocking = false))
    extraCliques.foreach(_.unpersist(blocking = false))
    stages.toSeq
  }

  /** Cache validity key: snapshot file identities (name+size+mtime), the sample size, and the config that can change
    * the ingest output (taxa allowlist, promoted prefixes, category/prefix drops). Name-gate lists live in Uniqueness
    * and are deliberately NOT part of the key.
    */
  private val RegistryVersion = "4" // v3: crossrefs as synonyms; v4: majority-vote crossrefs (per-genome taxid column)

  private def fingerprint(
      opts: Options,
      taxa: Config.TaxaConfig,
      promotePrefixes: Set[String],
      filters: Config.FilterConfig
  ): String = {
    val digest = java.security.MessageDigest.getInstance("SHA-256")
    def put(s: String): Unit = digest.update(s.getBytes("UTF-8"))
    opts.babelDir.foreach { dir =>
      Seq("compendia", "synonyms").foreach { sub =>
        val d = dir / sub
        if (os.exists(d))
          os.list(d).filter(os.isFile).sortBy(_.last).foreach { f =>
            val attrs = os.stat(f)
            put(s"${f.last}:${attrs.size}:${attrs.mtime}")
          }
      }
    }
    opts.customDir.foreach { dir =>
      if (os.exists(dir))
        os.list(dir).filter(os.isFile).sortBy(_.last).foreach { f =>
          val attrs = os.stat(f)
          put(s"${f.last}:${attrs.size}:${attrs.mtime}")
        }
    }
    opts.gtdbDir.foreach { dir =>
      if (os.exists(dir))
        os.list(dir).filter(os.isFile).sortBy(_.last).foreach { f =>
          val attrs = os.stat(f)
          put(s"${f.last}:${attrs.size}:${attrs.mtime}")
        }
    }
    // The cache fingerprint cannot see code: bump this whenever source composition or source
    // logic changes (adding gtdb was such a change) or stale caches silently keep old data.
    put(s"registry=$RegistryVersion")
    put(opts.sample.map(_.toString).getOrElse("nosample"))
    put(taxa.allowlist.toSeq.sorted.mkString(","))
    put(promotePrefixes.toSeq.sorted.mkString(","))
    put(filters.dropCategories.toSeq.sorted.mkString(","))
    put(
      filters.dropCuriePrefixesByCategory.toSeq
        .sortBy(_._1)
        .map { case (k, v) => s"$k=${v.toSeq.sorted.mkString("|")}" }
        .mkString(",")
    )
    put(filters.excludeCurieRegex.mkString(","))
    digest.digest().map(b => f"$b%02x").mkString
  }
}
