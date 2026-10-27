package biolinkamalgam

import biolinkamalgam.config.Config
import biolinkamalgam.export.Manifest
import biolinkamalgam.ingest.{
  Aria2BulkDownloader,
  ChunkedJdkBulkDownloader,
  FixtureSnapshotProvider,
  JdkBulkDownloader,
  RemoteSnapshotProvider
}
import biolinkamalgam.spark.SparkSupport
import biolinkamalgam.transform.SynonymScrub
import mainargs.main
import org.apache.spark.sql.functions.{col, rand, row_number}

/** CLI entry point. Subcommands: run — full pipeline: ingest → merge → arbitrate → export bench — timed pipeline run
  * recording per-stage metrics to bench/history/
  */
object Main {

  /** Resolve (master, spark conf) from a profile. The reserved `master` key is pulled out of the conf; an explicit CLI
    * `--master` wins over the profile's.
    */
  private def sparkSettings(
      profile: Option[String],
      cliMaster: Option[String]
  ): (Option[String], Map[String, String]) = {
    val profileConf: Map[String, String] = profile match {
      case Some(name) => Config.loadProfile(os.pwd / "config" / "profiles.yaml", name)
      case None       => Map.empty
    }
    (cliMaster.orElse(profileConf.get("master")), profileConf - "master")
  }

  @main def run(
      babel: Option[String],
      custom: Option[String],
      out: String,
      taxaConfig: String = "config/taxa.yaml",
      promoteConfig: String = "config/promote_prefixes.yaml",
      filtersConfig: String = "config/filters.yaml",
      download: Option[String] = None,
      downloadConfig: String = "config/download.yaml",
      snapshotDir: Option[String] = None,
      master: Option[String] = None,
      profile: Option[String] = None,
      sample: Option[Long] = None,
      ingestCache: Option[String] = None,
      gtdb: Option[String] = None,
      clinvar: Option[String] = None,
      obo: Option[String] = None,
      oboConfig: String = "config/obo_sources.yaml",
      curieTaxaConfig: String = "config/curie_taxa.yaml",
      noTaxonInference: mainargs.Flag = mainargs.Flag(false),
      audit: mainargs.Flag = mainargs.Flag(false),
      simpleExport: mainargs.Flag = mainargs.Flag(false)
  ): Unit = {
    val babelDir: Option[os.Path] = (babel, download) match {
      case (Some(dir), _)        => Some(new FixtureSnapshotProvider(os.Path(dir, os.pwd)).ensureSnapshot(os.pwd))
      case (None, Some(version)) =>
        // Not under out/: mill owns that dir and `mill clean` would wipe a
        // multi-GB snapshot.
        val dir = snapshotDir.map(os.Path(_, os.pwd)).getOrElse(os.pwd / "data" / "snapshots" / version)
        val dl = Config.loadDownload(os.Path(downloadConfig, os.pwd))
        val engine = dl.engine match {
          case "aria2"       => new Aria2BulkDownloader(dl.aria2)
          case "jdk-chunked" => new ChunkedJdkBulkDownloader(dl.chunkedConnectionsPerFile, dl.jdkConcurrency)
          case _             => new JdkBulkDownloader(dl.jdkConcurrency)
        }
        Some(new RemoteSnapshotProvider(version, engine, dl.baseUrl, dl.excludeFileRegex).ensureSnapshot(dir))
      case (None, None) => None
    }

    val (resolvedMaster, conf) = sparkSettings(profile, master)
    val spark = SparkSupport.session("biolinkamalgam", resolvedMaster, conf)
    try {
      val stages = Pipeline.run(
        spark,
        Pipeline.Options(
          babelDir = babelDir,
          customDir = custom.map(d => os.Path(d, os.pwd)),
          gtdbDir = gtdb.map(d => os.Path(d, os.pwd)),
          clinvarFile = clinvar.map(f => os.Path(f, os.pwd)),
          oboDir = obo.map(d => os.Path(d, os.pwd)),
          oboConfig = Some(os.Path(oboConfig, os.pwd)),
          curieTaxaConfig = if (noTaxonInference.value) None else Some(os.Path(curieTaxaConfig, os.pwd)),
          outDir = os.Path(out, os.pwd),
          taxaConfig = os.Path(taxaConfig, os.pwd),
          promoteConfig = os.Path(promoteConfig, os.pwd),
          filtersConfig = os.Path(filtersConfig, os.pwd),
          simpleExport = simpleExport.value,
          sample = sample,
          ingestCache = ingestCache.map(p => os.Path(p, os.pwd)),
          audit = audit.value,
          datasetMeta = Map(
            "babel" -> download.getOrElse(babel.map(d => s"local:$d").getOrElse("none")),
            "custom" -> custom.getOrElse("none"),
            "gtdb" -> gtdb.getOrElse("none"),
            "clinvar" -> clinvar.getOrElse("none"),
            "obo" -> obo.getOrElse("none")
          )
        )
      )
      Manifest.write(
        os.Path(out, os.pwd) / "run_manifest.json",
        stages,
        Map(
          "master" -> spark.sparkContext.master,
          "babel" -> babelDir.map(_.toString).getOrElse("none"),
          "custom" -> custom.getOrElse("none"),
          "gtdb" -> gtdb.getOrElse("none")
        )
      )
      stages.foreach(s => println(f"${s.name}%-12s ${s.rows}%12d rows  ${s.millis}%8d ms"))
    } finally spark.stop()
  }

  @main def bench(
      babel: Option[String],
      custom: Option[String],
      out: String = "bench/history",
      taxaConfig: String = "config/taxa.yaml",
      promoteConfig: String = "config/promote_prefixes.yaml",
      filtersConfig: String = "config/filters.yaml",
      master: Option[String] = None,
      profile: Option[String] = Some("local"),
      gtdb: Option[String] = None,
      tag: String = ""
  ): Unit = {
    val (resolvedMaster, conf) = sparkSettings(profile, master)
    val spark = SparkSupport.session("biolinkamalgam-bench", resolvedMaster, conf)
    try {
      val tmpOut = os.temp.dir(prefix = "biolinkamalgam-bench")
      try {
        val stages = Pipeline.run(
          spark,
          Pipeline.Options(
            babelDir = babel.map(d => os.Path(d, os.pwd)),
            customDir = custom.map(d => os.Path(d, os.pwd)),
            gtdbDir = gtdb.map(d => os.Path(d, os.pwd)),
            outDir = tmpOut,
            taxaConfig = os.Path(taxaConfig, os.pwd),
            promoteConfig = os.Path(promoteConfig, os.pwd),
            filtersConfig = os.Path(filtersConfig, os.pwd)
          )
        )
        val ts = new java.text.SimpleDateFormat("yyyyMMdd-HHmmss").format(new java.util.Date())
        val dest = os.Path(out, os.pwd) / s"$ts${if (tag.nonEmpty) s"-$tag" else ""}.json"
        Manifest.write(
          dest,
          stages,
          Map(
            "master" -> spark.sparkContext.master,
            "cores" -> Runtime.getRuntime.availableProcessors().toString
          )
        )
        println(s"bench recorded → $dest")
        stages.foreach(s => println(f"${s.name}%-12s ${s.rows}%12d rows  ${s.millis}%8d ms"))
      } finally os.remove.all(tmpOut)
    } finally spark.stop()
  }

  /** Time one URL through each download engine — the evidence for which engine download.yaml should name. */
  @main def benchDownload(
      url: String,
      out: String,
      connections: Int = 16,
      files: Int = 8
  ): Unit = {
    val dest = os.Path(out, os.pwd)
    os.makeDir.all(dest)
    Seq(
      "aria2" -> new Aria2BulkDownloader(
        Config.Aria2Config(connectionsPerFile = connections, split = connections, concurrentFiles = files)
      ),
      "jdk-chunked" -> new ChunkedJdkBulkDownloader(connections, files),
      "jdk" -> new JdkBulkDownloader(files)
    ).foreach { case (name, engine) =>
      val target = dest / s"$name" / "bench.bin"
      os.remove.all(target / os.up)
      val t0 = System.nanoTime()
      engine.downloadAll(Seq(os.sub / "bench.bin" -> url), target / os.up)
      val ms = (System.nanoTime() - t0) / 1000000
      val size = os.size(target)
      println(f"$name%-12s $size%12d bytes in ${ms}%8d ms  ${size / 1048576.0 / (ms / 1000.0)}%8.1f MB/s")
    }
  }

  /** Pre-publication sanity checks on a finished run: schema, class drops, the uniqueness invariant, and the gates. */
  @main def validate(
      out: String,
      taxaConfig: String = "config/taxa.yaml",
      master: Option[String] = None,
      profile: Option[String] = None
  ): Unit = {
    val (resolvedMaster, conf) = sparkSettings(profile, master)
    val spark = SparkSupport.session("biolinkamalgam-validate", resolvedMaster.orElse(Some("local[*]")), conf)
    import org.apache.spark.sql.functions.{col, desc, explode, lower, size}
    var failed = false
    def check(label: String, ok: Boolean, detail: String = ""): Unit = {
      println(s"${if (ok) "ok  " else "FAIL"}  $label%-48s $detail")
      if (!ok) failed = true
    }
    try {
      val dir = os.Path(out, os.pwd)
      val df = spark.read.parquet((dir / "biolinkamalgam.parquet").toString)

      check(
        "schema == CURIE,PREFERRED_NAME,CLASS,SYNONYMS,TAXA",
        df.schema.fieldNames.mkString(",") == "CURIE,PREFERRED_NAME,CLASS,SYNONYMS,TAXA",
        df.schema.fieldNames.mkString(",")
      )
      val rows = df.count()
      check("row count > 0", rows > 0, s"rows=$rows")
      check("CURIEs unique", rows - df.select("CURIE").distinct().count() == 0)

      val exploded = df.select(col("CLASS"), explode(col("SYNONYMS")).as("s"))
      // The guarantee is scoped by (class, taxon bucket) and TAXA ships the raw taxa, not the
      // bucket, so a name shared by same-class cliques in DIFFERENT taxon scopes is legal here.
      // Report instead of fail; a runaway number would signal an arbitration bug.
      val crossCurie = exploded.groupBy("CLASS", "s").count().filter(col("count") > 1)
      val crossCurieCount = crossCurie.count()
      val crossSample = crossCurie
        .orderBy(desc("count"))
        .limit(3)
        .collect()
        .map(r => s"${r.getAs[String]("s")} x${r.getAs[Long]("count")} ${r.getAs[String]("CLASS")}")
        .mkString("; ")
      check(
        "one-CURIE-per-(class, synonym) [informational]",
        crossCurieCount <= 5000000,
        s"shared-by-$crossCurieCount names; worst: $crossSample"
      )
      // The top-taxa cutoff (taxa.yaml taxon_allowlist, fullmap parity): any non-OrganismTaxon row that carries
      // taxa must carry at least one allowlisted taxon. Taxonless rows are legal; their count is reported.
      val allowlist = Config.loadTaxa(os.Path(taxaConfig, os.pwd)).allowlist
      if (allowlist.nonEmpty) {
        val outsideCount = Validate.taxonCutoffOutside(df, allowlist)
        val outsideSample = Validate
          .taxonCutoffOutsideRows(df, allowlist)
          .limit(3)
          .collect()
          .map(r => r.getAs[String]("CURIE"))
          .mkString(", ")
        check(
          s"taxa within top-${allowlist.size} cutoff",
          outsideCount == 0,
          s"$outsideCount outside; e.g. $outsideSample"
        )
      }
      val withTaxa = df.filter(size(col("TAXA")) > 0).count()
      check("TAXA present [informational]", true, s"$withTaxa of $rows rows carry taxa")
      check("TAXA values are NCBITaxon CURIEs", Validate.taxaShapeViolations(df) == 0)
      check(
        "Publication/Agent absent",
        df.filter(col("CLASS").isin("biolink:Publication", "biolink:Agent")).count() == 0
      )
      val numeric = exploded.filter(col("s").rlike("^[+-]?([0-9]+\\.?[0-9]*|\\.[0-9]+)([eE][+-]?[0-9]+)?$"))
      val numericCount = numeric.count()
      val numericSample = numeric.limit(5).collect().map(_.getAs[String]("s")).mkString(", ")
      check("no numeric-only synonyms", numericCount == 0, s"$numericCount; e.g. $numericSample")
      check("no 'language' synonyms", exploded.filter("lower(s) like '%language%'").count() == 0)
      check(
        "no dead-term synonyms",
        exploded
          .filter(lower(col("s")).isin("unknown", "none", "null", "the", "and", "expression", "symbol"))
          .count() == 0
      )

      println("\ntop classes:")
      df.groupBy("CLASS").count().orderBy(desc("count")).show(15, false)
      println(
        "synonyms seen under more than one CLASS (legal across classes): " +
          exploded.groupBy("s").count().filter(col("count") > 1).count()
      )

      if (failed) {
        println("VALIDATION FAILED")
        sys.exit(1)
      }
      println("VALIDATION PASSED")
    } finally spark.stop()
  }

  /** Pre-publication taxa checks, extracted so they are unit-testable without a Spark CLI run. */
  object Validate {
    import org.apache.spark.sql.functions.{array_intersect, col, explode, size, typedLit}

    /** Rows whose taxa are all outside the allowlist (the fullmap-parity cutoff). An empty allowlist disables the check
      * (same convention as Filters.taxonAllowlisted).
      */
    def taxonCutoffOutsideRows(
        df: org.apache.spark.sql.DataFrame,
        allowlist: Set[String]
    ): org.apache.spark.sql.DataFrame =
      if (allowlist.isEmpty) df.limit(0)
      else
        df.filter(
          col("CLASS") =!= "biolink:OrganismTaxon" && size(col("TAXA")) > 0 &&
            size(array_intersect(col("TAXA"), typedLit(allowlist.toSeq.sorted))) === 0
        )

    def taxonCutoffOutside(df: org.apache.spark.sql.DataFrame, allowlist: Set[String]): Long =
      taxonCutoffOutsideRows(df, allowlist).count()

    /** Null or non-NCBITaxon-CURIE elements anywhere in TAXA. */
    def taxaShapeViolations(df: org.apache.spark.sql.DataFrame): Long =
      df.select(explode(col("TAXA")).as("t"))
        .filter(col("t").isNull || !col("t").rlike("^NCBITaxon:[1-9][0-9]*$"))
        .count()
  }

  /** Report-only synonym diagnostics. Never writes or modifies the dataset. Lexical flags need review. */
  @main def scrub(
      out: String,
      scrubConfig: String = "config/synonym_scrub.yaml",
      master: Option[String] = None,
      profile: Option[String] = None
  ): Unit = {
    val (resolvedMaster, conf) = sparkSettings(profile, master)
    val spark = SparkSupport.session("biolinkamalgam-scrub", resolvedMaster, conf)
    try {
      val dir = os.Path(out, os.pwd)
      val df = spark.read.parquet((dir / "biolinkamalgam.parquet").toString)
      val cfg = Config.loadScrub(os.Path(scrubConfig, os.pwd))
      val scored = SynonymScrub
        .cohesionTable(df, cfg)
        .persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
      val flagged = SynonymScrub
        .flags(df, cfg, scored)
        .persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
      val counts = flagged.groupBy("signal").count().orderBy("signal")
      counts.show(false)
      val total = flagged.count()
      println(s"diagnostic signal rows (not unique synonyms): $total")
      println(
        s"cohesion scores only cliques with ${cfg.minCliqueMembers}-${cfg.maxCliqueMembers} names; others are skipped"
      )

      val reportDir = dir / "scrub-report"
      os.makeDir.all(reportDir)
      val sampled = flagged
        .withColumn(
          "rn",
          row_number().over(org.apache.spark.sql.expressions.Window.partitionBy(col("signal")).orderBy(rand(42L)))
        )
        .filter(col("rn") <= cfg.sampleExamples)
        .drop("rn")
      counts
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .option("sep", "\t")
        .csv((reportDir / "counts").toString)
      sampled
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .option("sep", "\t")
        .csv((reportDir / "samples").toString)
      println(s"report: $reportDir")

      flagged.unpersist(blocking = false)
      scored.unpersist(blocking = false)
      println("report only: dataset unchanged; flags are not proof of incorrectness")
    } finally spark.stop()
  }

  /** Benchmark a canonical clique parquet sample with the same narrow boundaries as production merging. */
  @main def benchMerge(
      input: String,
      out: String,
      taxaConfig: String = "config/taxa.yaml",
      master: Option[String] = None,
      profile: Option[String] = None
  ): Unit = {
    val dir = os.Path(out, os.pwd)
    require(!os.exists(dir), s"benchmark output already exists: $dir")
    os.makeDir.all(dir)
    val (resolvedMaster, conf) = sparkSettings(profile, master)
    val spark = SparkSupport.session("biolinkamalgam-merge-benchmark", resolvedMaster, conf)
    try {
      val start = System.nanoTime()
      val cliques = spark.read.schema(biolinkamalgam.schema.Schemas.clique).parquet(os.Path(input, os.pwd).toString)
      val result = biolinkamalgam.transform.CliqueMerge(
        cliques,
        Config.loadTaxa(os.Path(taxaConfig, os.pwd)).majorTaxa,
        (name, frame) => {
          val path = dir / name
          frame.write.parquet(path.toString)
          spark.read.parquet(path.toString)
        }
      )
      result.cliques.write.parquet((dir / "merged").toString)
      println(
        s"merge benchmark: input=${cliques.count()} output=${spark.read.parquet((dir / "merged").toString).count()} " +
          s"absorbed=${result.merges.count()} elapsed_ms=${(System.nanoTime() - start) / 1000000}"
      )
    } finally spark.stop()
  }

  /** Typed crossref graph statistics, never equivalence or an automated deletion decision. */
  @main def crossrefs(
      out: String,
      top: Int = 200,
      master: Option[String] = None,
      profile: Option[String] = None
  ): Unit = {
    require(top >= 1 && top <= 10000, "top must be 1-10000")
    val (resolvedMaster, conf) = sparkSettings(profile, master)
    val spark = SparkSupport.session("biolinkamalgam-crossrefs", resolvedMaster, conf)
    try {
      val dir = os.Path(out, os.pwd)
      val edges = biolinkamalgam.transform.CrossrefAnalysis
        .edges(spark.read.parquet((dir / "crossrefs").toString)) // every per-predicate parquet
        .persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
      val report = dir / "crossref-report"
      os.makeDir.all(report)
      biolinkamalgam.export.Tsv
        .writeSingleFile(biolinkamalgam.transform.CrossrefAnalysis.summary(edges), report / "summary.tsv")
      biolinkamalgam.export.Tsv
        .writeSingleFile(biolinkamalgam.transform.CrossrefAnalysis.highDegree(edges, top), report / "degree.tsv")
      edges.unpersist(blocking = false)
      println(s"crossref report: $report (typed associations; high degree is not proof of error)")
    } finally spark.stop()
  }

  def main(args: Array[String]): Unit = mainargs.ParserForMethods(this).runOrExit(args)
}
