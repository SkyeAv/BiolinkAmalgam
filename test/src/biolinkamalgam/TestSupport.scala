package biolinkamalgam

import org.apache.spark.sql.SparkSession

/** Shared test plumbing: classpath-resource path resolution and one local Spark session per forked test JVM (mill forks
  * JVMs for parallelism; suites within a JVM share this).
  */
object TestSupport {

  /** Absolute os.Path of a test resource (copied onto the test classpath by mill). */
  def resource(name: String): os.Path = {
    val url = getClassResource(name)
    os.Path(java.nio.file.Paths.get(url.toURI))
  }

  private def getClassResource(name: String): java.net.URL = {
    val url = Option(getClass.getResource(name))
    url.getOrElse(throw new IllegalArgumentException(s"test resource not found: $name"))
  }

  lazy val miniSnapshot: os.Path = resource("/snapshots/mini")
  lazy val customDir: os.Path = resource("/custom")
  lazy val taxaConfig: os.Path = resource("/config/taxa.yaml")
  lazy val promoteConfig: os.Path = resource("/config/promote_prefixes.yaml")

  lazy val majorTaxa: Set[String] = config.Config.loadTaxa(taxaConfig)
  lazy val promotePrefixes: Set[String] = config.Config.loadPromotePrefixes(promoteConfig)

  /** Local, quiet, tiny-shuffle session for unit/integration suites. */
  lazy val spark: SparkSession = {
    val s = SparkSession
      .builder()
      .appName("biolinkamalgam-test")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.adaptive.enabled", "true")
      .getOrCreate()
    s.sparkContext.setLogLevel("WARN")
    s
  }
}
