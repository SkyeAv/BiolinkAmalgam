package biolinkamalgam.transform

import biolinkamalgam.config.Config
import biolinkamalgam.schema.Schemas
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/** Clique merging by preferred-name string equality, scoped by (category, taxon_scope).
  *
  * Rules:
  *   - Preferred-name matching is strip-only (NameKey.stripKey) — never stemmed.
  *   - Empty preferred names never merge.
  *   - Taxa are bucketed through the configured major-taxa list: major taxa keep their own scope, long-tail taxa share
  *     "other-taxa", no-taxon cliques share "no-taxon".
  *   - Winner = most synonyms (name count), then identifier count, then source priority (lower wins), then smallest
  *     leader CURIE — a total order, so reruns reproduce.
  *   - The loser is absorbed: its names/taxa/identifier count fold into the winner, its leader CURIE is omitted from
  *     the output.
  *
  * Known corner (documented, rare): with multi-taxon cliques, a clique can win its group in one scope and lose in
  * another. Such a clique is treated as a loser and is excluded from winner candidacy everywhere; a loser whose
  * group-max is itself a loser passes through unmerged rather than merging across scopes.
  */
object CliqueMerge {

  case class Result(cliques: DataFrame, merges: DataFrame)

  def apply(cliques: DataFrame, majorTaxa: Set[String]): Result = {
    val spark = cliques.sparkSession
    import spark.implicits._

    val scopeUdf = udf { taxa: Seq[String] =>
      Config.taxonScopes(Option(taxa).getOrElse(Seq.empty), majorTaxa)
    }

    val rank = Seq(
      col("name_count").desc,
      col("identifier_count").desc,
      col("priority").asc,
      col("leader").asc
    )
    val groupKeys = Seq("pref_key", "category", "taxon_scope")

    val keyed = cliques
      .withColumn("pref_key", NameKey.stripKey(col("preferred_name")))
      .withColumn("name_count", size(col("names")))

    // One row per (clique, scope); cliques with empty preferred names stay out of merging.
    val scoped = keyed
      .filter(length(col("pref_key")) > 0)
      .withColumn("taxon_scope", explode(scopeUdf(col("taxa"))))

    val win = Window.partitionBy(groupKeys.map(col): _*).orderBy(rank: _*)
    val ranked = scoped.withColumn("rn", row_number().over(win))

    // Only groups with more than one member can merge.
    val groupSizes = ranked.groupBy(groupKeys.map(col): _*).count().filter(col("count") > 1)
    val contested = ranked.join(groupSizes.drop("count"), groupKeys)

    val loserLeaders = contested.filter(col("rn") > 1).select(col("leader")).distinct()

    // Winners: group-max cliques that are not themselves losers anywhere.
    val winners = contested
      .filter(col("rn") === 1)
      .join(loserLeaders, Seq("leader"), "left_anti")

    // Pair each loser with the winners in its scope groups; keep the best-ranked winner.
    val loserCols = contested
      .filter(col("rn") > 1)
      .select(
        col("leader").as("loser_leader"),
        col("pref_key"),
        col("category"),
        col("taxon_scope"),
        col("names").as("loser_names"),
        col("taxa").as("loser_taxa"),
        col("identifiers").as("loser_identifiers"),
        col("identifier_count").as("loser_ids")
      )
    val pairs = loserCols.join(
      winners.select(
        col("leader").as("winner_leader"),
        col("pref_key"),
        col("category"),
        col("taxon_scope"),
        col("name_count").as("winner_name_count"),
        col("identifier_count").as("winner_id_count"),
        col("priority").as("winner_priority")
      ),
      groupKeys
    )
    val bestWin = Window
      .partitionBy(col("loser_leader"))
      .orderBy(
        col("winner_name_count").desc,
        col("winner_id_count").desc,
        col("winner_priority").asc,
        col("winner_leader").asc
      )
    val best = pairs.withColumn("pick", row_number().over(bestWin)).filter(col("pick") === 1)

    val merges = best.select(
      col("pref_key").as("matched_preferred_name"),
      col("category"),
      col("taxon_scope"),
      col("winner_leader"),
      col("loser_leader").as("absorbed_leader"),
      col("winner_id_count"),
      col("loser_ids").as("absorbed_id_count"),
      col("winner_name_count"),
      size(col("loser_names")).as("absorbed_name_count")
    )

    // Fold losers into winners.
    val absorbed = best
      .groupBy("winner_leader")
      .agg(
        flatten(collect_list("loser_names")).as("absorbed_names"),
        flatten(collect_list("loser_taxa")).as("absorbed_taxa"),
        flatten(collect_list("loser_identifiers")).as("absorbed_identifiers")
      )

    val allLosers = best.select(col("loser_leader").as("leader")).distinct()

    val surviving = keyed
      .join(allLosers, Seq("leader"), "left_anti")
      .join(absorbed, col("leader") === col("winner_leader"), "left")
      .select(
        col("leader"),
        col("preferred_name"),
        col("category"),
        array_distinct(
          concat(col("taxa"), coalesce(col("absorbed_taxa"), array()))
        ).as("taxa"),
        array_distinct(
          concat(col("names"), coalesce(col("absorbed_names"), array()))
        ).as("names"),
        array_distinct(
          concat(col("identifiers"), coalesce(col("absorbed_identifiers"), array()))
        ).as("identifiers"),
        col("source"),
        col("priority")
      )
      // The count is derived from the unioned identifier list, never summed:
      // loser and winner lists can overlap.
      .withColumn("identifier_count", size(col("identifiers")))

    Result(surviving, merges)
  }
}
