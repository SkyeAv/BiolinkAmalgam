package biolinkamalgam.spark

import org.apache.spark.sql.SparkSession

/** Spark session construction: probe the local standalone master (spark://127.0.0.1:7077, from the NixOS spark-master +
  * spark-worker systemd services) with a short TCP timeout; fall back to local[*] with a logged warning.
  */
object SparkSupport {

  val DefaultMaster = "spark://127.0.0.1:7077"

  def probeMaster(masterUrl: String, timeoutMs: Int = 500): Boolean = {
    val m = "spark://([^:]+):(\\d+)".r.unapplySeq(masterUrl)
    m match {
      case Some(Seq(host, port)) =>
        try {
          val socket = new java.net.Socket()
          socket.connect(new java.net.InetSocketAddress(host, port.toInt), timeoutMs)
          socket.close()
          true
        } catch { case _: Exception => false }
      case _ => false
    }
  }

  /** Build a session. `master = None` probes the local service; `Some("local[*]")` etc. forces a master. `conf` carries
    * profile + CLI overrides.
    */
  def session(
      appName: String,
      master: Option[String],
      conf: Map[String, String]
  ): SparkSession = {
    val resolved = master match {
      case Some(m) => m
      case None =>
        if (probeMaster(DefaultMaster)) DefaultMaster
        else {
          System.err.println(
            s"[biolinkamalgam] no Spark master at $DefaultMaster — falling back to local[*]"
          )
          "local[*]"
        }
    }
    val builder = SparkSession
      .builder()
      .appName(appName)
      .master(resolved)
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.parquet.enableVectorizedReader", "true")
    // On a standalone cluster the executors are a different OS user than the driver;
    // driver-created job dirs (e.g. _temporary) must be writable by them on the
    // shared filesystem.
    if (resolved.startsWith("spark://"))
      builder.config("spark.hadoop.fs.permissions.umask-mode", "000")
    conf.foreach { case (k, v) => builder.config(k, v) }
    builder.getOrCreate()
  }
}
