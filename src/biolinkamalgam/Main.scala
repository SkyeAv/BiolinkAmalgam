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
import mainargs.main

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
          outDir = os.Path(out, os.pwd),
          taxaConfig = os.Path(taxaConfig, os.pwd),
          promoteConfig = os.Path(promoteConfig, os.pwd),
          filtersConfig = os.Path(filtersConfig, os.pwd),
          simpleExport = simpleExport.value,
          sample = sample,
          ingestCache = ingestCache.map(p => os.Path(p, os.pwd)),
          datasetMeta = Map(
            "babel" -> download.getOrElse(babel.map(d => s"local:$d").getOrElse("none")),
            "custom" -> custom.getOrElse("none")
          )
        )
      )
      Manifest.write(
        os.Path(out, os.pwd) / "run_manifest.json",
        stages,
        Map(
          "master" -> spark.sparkContext.master,
          "babel" -> babelDir.map(_.toString).getOrElse("none"),
          "custom" -> custom.getOrElse("none")
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
      master: Option[String] = None,
      profile: Option[String] = None
  ): Unit = {
    val (resolvedMaster, conf) = sparkSettings(profile, master)
    val spark = SparkSupport.session("biolinkamalgam-validate", resolvedMaster.orElse(Some("local[*]")), conf)
    import spark.implicits._
    import org.apache.spark.sql.functions.{col, explode, lower}
    var failed = false
    def check(label: String, ok: Boolean, detail: String = ""): Unit = {
      println(s"${if (ok) "ok  " else "FAIL"}  $label%-48s $detail")
      if (!ok) failed = true
    }
    try {
      val dir = os.Path(out, os.pwd)
      val df = spark.read.parquet((dir / "biolinkamalgam.parquet").toString)

      check(
        "schema == CURIE,PREFERRED_NAME,CLASS,SYNONYMS",
        df.schema.fieldNames.mkString(",") == "CURIE,PREFERRED_NAME,CLASS,SYNONYMS",
        df.schema.fieldNames.mkString(",")
      )
      val rows = df.count()
      check("row count > 0", rows > 0, s"rows=$rows")
      check("CURIEs unique", rows - df.select("CURIE").distinct().count() == 0)

      val exploded = df.select("CLASS", explode(col("SYNONYMS")).as("s"))
      check(
        "one-CURIE-per-(class, synonym)",
        exploded.groupBy("CLASS", "s").count().filter(col("count") > 1).count() == 0
      )
      check(
        "Publication/Agent absent",
        df.filter(col("CLASS").isin("biolink:Publication", "biolink:Agent")).count() == 0
      )
      check(
        "no numeric-only synonyms",
        exploded.filter("s rlike '^[+-]?([0-9]+\\.?[0-9]*|\\.[0-9]+)([eE][+-]?[0-9]+)?$$'").count() == 0
      )
      check("no 'language' synonyms", exploded.filter("lower(s) like '%language%'").count() == 0)
      check(
        "no dead-term synonyms",
        exploded.filter(lower(col("s")).isin("unknown", "none", "null", "the", "and", "expression", "symbol"))
          .count() == 0
      )

      println("
top classes:")
      df.groupBy("CLASS").count().orderBy($"count".desc).show(15, false)
      println(s"synonyms seen under more than one CLASS (legal across classes): " +
        exploded.groupBy("s").count().filter(col("count") > 1).count())

      if (failed) {
        println("VALIDATION FAILED")
        sys.exit(1)
      }
      println("VALIDATION PASSED")
    } finally spark.stop()
  }

  def main(args: Array[String]): Unit = mainargs.ParserForMethods(this).runOrExit(args)
}
