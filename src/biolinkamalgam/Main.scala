package biolinkamalgam

import biolinkamalgam.config.Config
import biolinkamalgam.export.Manifest
import biolinkamalgam.ingest.{FixtureSnapshotProvider, RemoteSnapshotProvider}
import biolinkamalgam.spark.SparkSupport
import mainargs.main

/** CLI entry point. Subcommands: run — full pipeline: ingest → merge → arbitrate → export bench — timed pipeline run
  * recording per-stage metrics to bench/history/
  */
object Main {

  private def sparkConf(profile: Option[String], extra: Map[String, String]): Map[String, String] = {
    val profileConf: Map[String, String] = profile match {
      case Some(name) => Config.loadProfile(os.pwd / "config" / "profiles.yaml", name)
      case None       => Map.empty
    }
    profileConf ++ extra
  }

  @main def run(
      babel: Option[String],
      custom: Option[String],
      out: String,
      taxaConfig: String = "config/taxa.yaml",
      promoteConfig: String = "config/promote_prefixes.yaml",
      download: Option[String] = None,
      master: Option[String] = None,
      profile: Option[String] = None,
      simpleExport: mainargs.Flag = mainargs.Flag(false)
  ): Unit = {
    val babelDir: Option[os.Path] = (babel, download) match {
      case (Some(dir), _) => Some(new FixtureSnapshotProvider(os.Path(dir, os.pwd)).ensureSnapshot(os.pwd))
      case (None, Some(version)) =>
        val dir = os.pwd / "out" / "snapshots" / version
        Some(new RemoteSnapshotProvider(version).ensureSnapshot(dir))
      case (None, None) => None
    }

    val spark = SparkSupport.session(
      "biolinkamalgam",
      master,
      sparkConf(profile, Map.empty)
    )
    try {
      val stages = Pipeline.run(
        spark,
        Pipeline.Options(
          babelDir = babelDir,
          customDir = custom.map(d => os.Path(d, os.pwd)),
          outDir = os.Path(out, os.pwd),
          taxaConfig = os.Path(taxaConfig, os.pwd),
          promoteConfig = os.Path(promoteConfig, os.pwd),
          simpleExport = simpleExport.value
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
      master: Option[String] = None,
      profile: Option[String] = Some("local"),
      tag: String = ""
  ): Unit = {
    val spark = SparkSupport.session("biolinkamalgam-bench", master, sparkConf(profile, Map.empty))
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
            promoteConfig = os.Path(promoteConfig, os.pwd)
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
