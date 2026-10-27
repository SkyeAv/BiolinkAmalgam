package biolinkamalgam.transform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/** Directed, typed graph statistics. Never infers equivalence or merges connected components. Degree means distinct
  * neighbors within one predicate, not an error probability or independent source support.
  */
object CrossrefAnalysis {
  private val Columns = Set("subject", "predicate", "object", "source")

  def edges(input: DataFrame): DataFrame = {
    require(Columns.subsetOf(input.columns.toSet), "crossrefs require subject, predicate, object, source")
    input
      .select("subject", "predicate", "object", "source")
      .filter(
        col("subject").isNotNull && col("object").isNotNull && col("predicate").isNotNull && col("source").isNotNull
      )
      .filter(
        length(col("subject")) > 0 && length(col("object")) > 0 && length(col("predicate")) > 0 && length(
          col("source")
        ) > 0
      )
      .distinct()
  }

  def summary(clean: DataFrame): DataFrame = clean
    .groupBy(
      col("predicate"),
      col("source"),
      split(col("subject"), ":")(0).as("subject_prefix"),
      split(col("object"), ":")(0).as("object_prefix")
    )
    .agg(
      count(lit(1)).as("edges"),
      count_distinct(col("subject")).as("subjects"),
      count_distinct(col("object")).as("objects")
    )
    .orderBy("predicate", "source", "subject_prefix", "object_prefix")

  /** Top N inbound/outbound degree nodes per predicate. Bounds apply to the report, not to degree computation. */
  def highDegree(clean: DataFrame, limitPerPredicate: Int): DataFrame = {
    require(limitPerPredicate >= 1 && limitPerPredicate <= 10000, "degree sample limit must be 1-10000")
    val pairs = clean.select("subject", "predicate", "object").distinct()
    val outbound = pairs
      .groupBy("predicate", "subject")
      .agg(count(lit(1)).as("neighbors"))
      .withColumnRenamed("subject", "node")
      .withColumn("direction", lit("out"))
    val inbound = pairs
      .groupBy("predicate", "object")
      .agg(count(lit(1)).as("neighbors"))
      .withColumnRenamed("object", "node")
      .withColumn("direction", lit("in"))
    val ranked = Window.partitionBy("predicate", "direction").orderBy(col("neighbors").desc, col("node"))
    outbound
      .unionByName(inbound)
      .filter(col("neighbors") > 1)
      .withColumn("rank", row_number().over(ranked))
      .filter(col("rank") <= limitPerPredicate)
      .select("predicate", "direction", "node", "neighbors", "rank")
      .orderBy("predicate", "direction", "rank")
  }
}
