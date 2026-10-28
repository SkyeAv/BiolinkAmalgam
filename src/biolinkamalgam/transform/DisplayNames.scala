package biolinkamalgam.transform

import biolinkamalgam.config.Config.FilterConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/** Identifier lookup parity with fullmap, and human-readable preferred names before merge/arbitration. Runs after the
  * ingest cache so the behavior also applies to cached and additive sources. Never adds local IDs or infers identity.
  */
object DisplayNames {
  private val Curie = "^[A-Za-z][A-Za-z0-9_.-]*:\\S.*$"

  def apply(cliques: DataFrame, prefixes: Set[String], filters: FilterConfig): DataFrame = {
    val promoted = filter(
      array_distinct(concat(array(col("leader")), col("identifiers"))),
      id =>
        id.isNotNull && id.rlike(Curie) &&
          (if (prefixes("*")) lit(true) else split(id, ":")(0).isin(prefixes.toSeq: _*))
    )
    val preferred = udf((current: String, names: Seq[String]) => {
      if (current != null && current.trim.nonEmpty && !current.trim.matches(Curie)) current
      else
        Option(names).toSeq.flatten
          .filter(n => n != null && !n.trim.matches(Curie) && TermGates.admitted(n, filters) != null)
          .sorted
          .headOption
          .getOrElse(current)
    })
    cliques
      .withColumn("names", array_distinct(concat(col("names"), promoted)))
      .withColumn("preferred_name", preferred(col("preferred_name"), col("names")))
  }
}
