package biolinkamalgam

import org.apache.spark.sql.SparkSession
import utest._

/** Smoke test against the real local standalone cluster. Only runs when BIOLINKAMALGAM_SPARK_MASTER is set — i.e. via
  * `make test-cluster`, which starts the services, sets the variable, and points spark.jars at the cluster assembly.
  *
  * Executors run as the `spark` system user, so fixtures and the output dir are staged under /tmp with
  * world-readable/writable permissions — anything under $HOME is invisible to them.
  */
object ClusterSuite extends TestSuite {

  private def stageFixtures(): (os.Path, os.Path, os.Path, os.Path) = {
    val staging =
      os.Path("/tmp") / s"biolinkamalgam-cluster-test-${java.lang.ProcessHandle.current().pid()}"
    os.makeDir.all(staging)
    os.copy(TestSupport.miniSnapshot, staging / "snapshot")
    os.copy(TestSupport.customDir, staging / "custom")
    os.copy(TestSupport.taxaConfig, staging / "taxa.yaml")
    os.copy(TestSupport.promoteConfig, staging / "promote_prefixes.yaml")
    os.copy(TestSupport.filtersConfig, staging / "filters.yaml")
    os.walk(staging).foreach(p => os.perms.set(p, if (os.isDir(p)) "rwxrwxrwx" else "rw-r--r--"))
    val out = staging / "out"
    os.makeDir.all(out)
    os.perms.set(out, "rwxrwxrwx")
    (staging / "snapshot", staging / "custom", staging, out)
  }

  val tests = Tests {

    test("mini fixture pipeline end-to-end on spark://127.0.0.1:7077") {
      sys.env.get("BIOLINKAMALGAM_SPARK_MASTER") match {
        case None =>
          println("ClusterSuite skipped — run via `make test-cluster`")
        case Some(master) =>
          val jar = sys.env.getOrElse(
            "BIOLINKAMALGAM_CLUSTER_JAR",
            throw new IllegalStateException(
              "BIOLINKAMALGAM_CLUSTER_JAR not set — run via `make test-cluster`"
            )
          )
          val (snapshot, custom, staging, out) = stageFixtures()
          val spark = SparkSession
            .builder()
            .appName("biolinkamalgam-cluster-test")
            .master(master)
            .config("spark.jars", jar)
            .config("spark.cores.max", "4")
            .config("spark.executor.memory", "2g")
            .config("spark.sql.shuffle.partitions", "8")
            // executors run as the `spark` user: driver-created job dirs (_temporary)
            // must be writable by them
            .config("spark.hadoop.fs.permissions.umask-mode", "000")
            .getOrCreate()
          try {
            val stages = Pipeline.run(
              spark,
              Pipeline.Options(
                babelDir = Some(snapshot),
                customDir = Some(custom),
                outDir = out,
                taxaConfig = staging / "taxa.yaml",
                promoteConfig = staging / "promote_prefixes.yaml",
                filtersConfig = staging / "filters.yaml"
              )
            )
            assert(stages.find(_.name == "arbitrate").get.rows == 13)
            val back = biolinkamalgam.export.DatasetFiles.cliques(spark, out)
            assert(back.count() == 13)
          } finally {
            spark.stop()
            os.remove.all(staging)
          }
      }
    }
  }
}
