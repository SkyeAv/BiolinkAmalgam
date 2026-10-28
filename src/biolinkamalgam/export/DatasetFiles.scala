package biolinkamalgam.export

import biolinkamalgam.schema.Schemas
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Read only finished top-level deliverables, never interrupted partition-write debris. */
object DatasetFiles {
  def files(dir: os.Path): Seq[os.Path] =
    if (!os.exists(dir)) Seq.empty
    else os.list(dir).filter(f => os.isFile(f) && f.last.endsWith(".parquet")).sortBy(_.last)

  def cliques(spark: SparkSession, out: os.Path): DataFrame = {
    val paths = files(out / "cliques")
    require(paths.nonEmpty, s"no clique parquet files under ${out / "cliques"}")
    spark.read.schema(Schemas.output).parquet(paths.map(_.toString): _*)
  }

  /** Restore source from the split for local graph diagnostics. Public files keep only the three uppercase columns. */
  def crossrefs(spark: SparkSession, out: os.Path): DataFrame = {
    val paths = files(out / "crossrefs")
    require(paths.nonEmpty, s"no crossref parquet files under ${out / "crossrefs"}")
    paths
      .map { f =>
        spark.read
          .schema(Schemas.crossrefOutput)
          .parquet(f.toString)
          .toDF("subject", "predicate", "object")
          .withColumn("source", lit(f.last.stripSuffix(".parquet")))
      }
      .reduce(_ unionByName _)
  }
}
