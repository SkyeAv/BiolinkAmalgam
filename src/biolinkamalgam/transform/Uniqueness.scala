package biolinkamalgam.transform

import biolinkamalgam.config.Config
import biolinkamalgam.config.Config.FilterConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/** The core rule: global synonym uniqueness scoped by (category, taxon_scope).
  *
  * A stripped name key (trim/lowercase/drop punctuation — NO stemming anywhere in this codebase) may live in exactly
  * ONE clique's SYNONYMS within a scope. The same name MAY recur across different categories or different taxon scopes
  * (the exception). Junk gates (TermGates) run before arbitration; output keeps the ORIGINAL strings.
  *
  * Winner order: preferred-name match (strip comparison), then most synonyms, then identifier count, then source
  * priority (lower wins), then smallest leader CURIE. Losers are logged to the conflicts TSV with their winner;
  * surviving names are re-aggregated into the canonical output shape.
  */
object Uniqueness {

  case class Result(output: DataFrame, conflicts: DataFrame)

  def apply(cliques: DataFrame, majorTaxa: Set[String], filters: FilterConfig): Result = {
    val scopeUdf = udf { taxa: Seq[String] =>
      Config.taxonScopes(Option(taxa).getOrElse(Seq.empty), majorTaxa)
    }
    val admitUdf = udf { name: String => TermGates.admitted(name, filters) }

    val exploded = cliques
      .withColumn("name", explode(col("names")))
      .filter(length(col("name")) > 0)
      // one UDF gates the name and yields its cleaned form; rejected rows get null and drop out here
      .withColumn("cleaned", admitUdf(col("name")))
      .filter(col("cleaned").isNotNull)
      .withColumn("key", NameKey.stripKey(col("cleaned")))
      .withColumn("taxon_scope", explode(scopeUdf(col("taxa"))))
      .withColumn("name_count", size(col("names")))
      .withColumn(
        "is_preferred",
        NameKey.stripKey(col("name")) === NameKey.stripKey(col("preferred_name"))
      )

    val keys = Seq("key", "category", "taxon_scope")
    val win = Window
      .partitionBy(keys.map(col): _*)
      .orderBy(
        col("is_preferred").desc,
        col("name_count").desc,
        col("identifier_count").desc,
        col("priority").asc,
        col("leader").asc
      )
    val ranked = exploded.withColumn("rn", row_number().over(win))
    val kept = ranked.filter(col("rn") === 1).drop("rn")
    val winnersByKey = kept
      .select((keys :+ "leader").map(col): _*)
      .withColumnRenamed("leader", "winner_curie")

    // Conflicts: losing rows whose loser is a DIFFERENT clique than the winner
    // (duplicate names inside one clique are deduped silently, not conflicts).
    val conflicts = ranked
      .filter(col("rn") > 1)
      .drop("rn")
      .join(winnersByKey, keys)
      .filter(col("leader") =!= col("winner_curie"))
      .select(
        col("name"),
        col("key").as("name_key"),
        col("category"),
        col("taxon_scope"),
        col("leader").as("loser_curie"),
        col("winner_curie")
      )
      .distinct()

    // Re-aggregate: one row per clique leader; NAMEs keep the ORIGINAL strings.
    // A name surviving in several taxon scopes produces identical rows, so the
    // set-collect dedups it.
    val surviving = kept
      .groupBy("leader")
      .agg(sort_array(collect_set("name")).as("SYNONYMS"))

    // Cliques whose names ALL lost arbitration (or failed the gates) still get
    // a row with empty SYNONYMS — one row per surviving clique, not per kept name.
    val output = cliques
      .select(col("leader"), col("preferred_name"), col("category"))
      .join(surviving, Seq("leader"), "left")
      .select(
        col("leader").as("CURIE"),
        col("preferred_name").as("PREFERRED_NAME"),
        col("category").as("CLASS"),
        coalesce(col("SYNONYMS"), array().cast("array<string>")).as("SYNONYMS")
      )

    Result(output, conflicts)
  }
}
