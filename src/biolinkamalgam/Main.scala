package biolinkamalgam

import biolinkamalgam.config.Config
import biolinkamalgam.export.Manifest
import biolinkamalgam.ingest.{Aria2BulkDownloader, FixtureSnapshotProvider, JdkBulkDownloader, RemoteSnapshotProvider}
import biolinkamalgam.spark.SparkSupport
import mainargs.main

/** CLI entry point. Subcommands: run — full pipeline: ingest → merge → arbitrate → export bench — timed pipeline run
  * recording per-stage metrics to bench/history/
  */
object Main {

  /** Resolve (master, spark conf) from a profile. The reserved `master` key is pulled out of the conf; an explicit
    * CLI `--master` wins over the profile's.
    */
  private def sparkSettings(profile: Option[String], cliMaster: Option[String]): (Option[String], Map[String, String]) = {
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
      simpleExport: mainargs.Flag = mainargs.Flag(false)
  ): Unit = {
    val babelDir: Option[os.Path] = (babel, download) match {
      case (Some(dir), _) => Some(new FixtureSnapshotProvider(os.Path(dir, os.pwd)).ensureSnapshot(os.pwd))
      case (None, Some(version)) =>
        // Not under out/: mill owns that dir and `mill clean` would wipe a
        // multi-GB snapshot.
        val dir = snapshotDir.map(os.Path(_, os.pwd)).getOrElse(os.pwd / "data" / "snapshots" / version)
        val dl = Config.loadDownload(os.Path(downloadConfig, os.pwd))
        val engine = dl.engine match {
          case "aria2" => new Aria2BulkDownloader(dl.aria2)
          case _       => new JdkBulkDownloader(dl.jdkConcurrency)
        }
        Some(new RemoteSnapshotProvider(version, engine, dl.baseUrl).ensureSnapshot(dir))
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

  def main(args: Array[String]): Unit = mainargs.ParserForMethods(this).runOrExit(args)
}
