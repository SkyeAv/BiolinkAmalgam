package biolinkamalgam

import biolinkamalgam.config.Config
import biolinkamalgam.export.{DatasetCard, Manifest, Parquet, Tsv}
import biolinkamalgam.source.Registry
import biolinkamalgam.transform.{CliqueMerge, Filters, Uniqueness}
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
      ingestCache: Option[os.Path] = None
  )

  def run(spark: SparkSession, opts: Options): Seq[Manifest.Stage] = {
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

    // [1] INGEST — union all registered sources into the canonical clique table,
    // then apply declarative category/prefix/taxon-allowlist filters. With
    // --ingest-cache, the result is materialized as parquet once; reruns that
    // keep the fingerprint (same snapshot files + same ingest-affecting config)
    // read it back in seconds instead of re-parsing 100GB+. Name-level gates
    // live in [3]+[4], so filters.yaml *gate* edits never invalidate the cache.
    val cliques = timed("ingest") {
      val sources = Registry.forConfig(opts.babelDir, opts.customDir, promotePrefixes)
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

    // [2] MERGE CLIQUES — preferred-name string matching, scoped by (category, taxon).
    val (merged, merges) = timed("merge") {
      val result = CliqueMerge(cliques, taxa.majorTaxa)
      result.cliques.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
      (result.cliques, result.merges) -> result.cliques.count()
    }

    // [3]+[4] GATE (junk filters inside) + ARBITRATE — global synonym uniqueness.
    val (output, conflicts) = timed("arbitrate") {
      val result = Uniqueness(merged, taxa.majorTaxa, filters)
      result.output.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
      (result.output, result.conflicts) -> result.output.count()
    }

    // [5] EXPORT — concurrent sink writes off the persisted arbitrated DataFrame.
    timed("export") {
      val pool = Executors.newFixedThreadPool(4)
      try {
        val parquetFut =
          pool.submit[Unit](() =>
            Parquet.writeSingleFile(output, opts.outDir / "biolinkamalgam.parquet", opts.simpleExport)
          )
        val conflictsFut = pool.submit[Unit](() => Tsv.writeSingleFile(conflicts, opts.outDir / "conflicts.tsv"))
        val mergesFut = pool.submit[Unit](() => Tsv.writeSingleFile(merges, opts.outDir / "merges.tsv"))
        parquetFut.get()
        conflictsFut.get()
        mergesFut.get()
        DatasetCard.write(opts.outDir, "biolinkamalgam.parquet", stages.toSeq, opts.datasetMeta)
        () -> output.count()
      } finally {
        pool.shutdown()
        pool.awaitTermination(1, TimeUnit.MINUTES)
      }
    }

    stages.toSeq
  }

  /** Cache validity key: snapshot file identities (name+size+mtime), the sample size, and the config that can
    * change the ingest output (taxa allowlist, promoted prefixes, category/prefix drops). Name-gate lists live in
    * Uniqueness and are deliberately NOT part of the key.
    */
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
    put(opts.sample.map(_.toString).getOrElse("nosample"))
    put(taxa.allowlist.toSeq.sorted.mkString(","))
    put(promotePrefixes.toSeq.sorted.mkString(","))
    put(filters.dropCategories.toSeq.sorted.mkString(","))
    put(filters.dropCuriePrefixesByCategory.toSeq.sortBy(_._1).map { case (k, v) => s"$k=${v.toSeq.sorted.mkString("|")}" }.mkString(","))
    put(filters.excludeCurieRegex.mkString(","))
    digest.digest().map(b => f"$b%02x").mkString
  }
}
