package biolinkamalgam.transform

import biolinkamalgam.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/** Clique merging by preferred-name string equality, scoped by (category, taxon_scope).
  *
  * Rules:
  *   - Preferred-name matching is strip-only (NameKey.stripKey) — never stemmed.
  *   - Empty preferred names and SequenceVariant cliques never merge.
  *   - Groups with disjoint identifiers under a shared namespace stay separate (label equality is not identity).
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

  /** True when both identifier lists use some prefix but share no identifier under it. */
  def namespaceConflict(a: Seq[String], b: Seq[String]): Boolean = groupNamespaceConflict(Seq(a, b))

  /** Each namespace needs one identifier shared by every member using it, not merely a bridge through the winner. */
  def groupNamespaceConflict(members: Seq[Seq[String]]): Boolean = {
    val common = scala.collection.mutable.Map.empty[String, Set[String]]
    members.exists { ids =>
      Option(ids)
        .getOrElse(Seq.empty)
        .filter(i => i != null && i.contains(':'))
        .groupBy(_.takeWhile(_ != ':'))
        .exists { case (prefix, values) =>
          val intersection = common.get(prefix).fold(values.toSet)(_.intersect(values.toSet))
          common.update(prefix, intersection)
          intersection.isEmpty
        }
    }
  }

  /** materialize breaks the reused merge DAG at narrow boundaries. Pipeline supplies disk-backed parquet stages; tests
    * can keep the pure default. Its owner must retain those stages until all returned frames are consumed.
    */
  def apply(
      cliques: DataFrame,
      majorTaxa: Set[String],
      materialize: (String, DataFrame) => DataFrame = (_, df) => df
  ): Result = {
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
      .filter(length(col("pref_key")) > 0 && col("category") =!= "biolink:SequenceVariant")
      .select(
        "leader",
        "pref_key",
        "category",
        "taxa",
        "identifiers",
        "identifier_count",
        "priority",
        "source",
        "name_count"
      )
      .withColumn("taxon_scope", explode(scopeUdf(col("taxa"))))

    // Find contested KEYS before ranking; do not sort 140M singleton rows or shuffle synonym payloads.
    val groupSizes = scoped.groupBy(groupKeys.map(col): _*).count().filter(col("count") > 1)
    val win = Window.partitionBy(groupKeys.map(col): _*).orderBy(rank: _*)
    val contested = materialize(
      "contested",
      scoped.join(groupSizes.drop("count"), groupKeys).withColumn("rn", row_number().over(win))
    )

    // Namespace veto: two cliques that both carry identifiers in some prefix but share none there are distinct
    // entities BABEL kept apart (two PUBCHEM.COMPOUND CIDs, two species' NCBIGene tRNA genes in __OTHER_TAXA__).
    // A preferred name shared by such cliques is ambiguous, so the whole group stays unmerged; merging a third
    // clique into an arbitrary one of them would be a guess. Cost: one small array check per contested pair.
    val conflictUdf = udf((a: Seq[String], b: Seq[String]) => CliqueMerge.namespaceConflict(a, b))
    val groupConflict = udf((members: Seq[Seq[String]]) => CliqueMerge.groupNamespaceConflict(members))
    val ambiguous = contested
      .groupBy(groupKeys.map(col): _*)
      .agg(collect_list(col("identifiers")).as("member_identifiers"))
      .filter(groupConflict(col("member_identifiers")))
      .select(groupKeys.map(col): _*)
    val mergeable = materialize("mergeable", contested.join(ambiguous, groupKeys, "left_anti"))

    val loserLeaders = mergeable.filter(col("rn") > 1).select(col("leader")).distinct()

    // Winners: group-max cliques that are not themselves losers anywhere.
    val winners = mergeable
      .filter(col("rn") === 1)
      .join(loserLeaders, Seq("leader"), "left_anti")

    // Pair each loser with the winners in its scope groups; keep the best-ranked winner.
    val loserCols = mergeable
      .filter(col("rn") > 1)
      .select(
        col("leader").as("loser_leader"),
        col("pref_key"),
        col("category"),
        col("taxon_scope"),
        col("name_count").as("loser_name_count"),
        col("priority").as("loser_priority"),
        col("source").as("loser_source"),
        col("identifiers").as("loser_identifiers"),
        col("identifier_count").as("loser_ids")
      )
    val pairs = loserCols
      .join(
        winners.select(
          col("leader").as("winner_leader"),
          col("pref_key"),
          col("category"),
          col("taxon_scope"),
          col("name_count").as("winner_name_count"),
          col("identifier_count").as("winner_id_count"),
          col("priority").as("winner_priority"),
          col("identifiers").as("winner_identifiers")
        ),
        groupKeys
      )
      .filter(!conflictUdf(col("loser_identifiers"), col("winner_identifiers"))) // multi-scope cliques
    val bestWin = Window
      .partitionBy(col("loser_leader"))
      .orderBy(
        col("winner_name_count").desc,
        col("winner_id_count").desc,
        col("winner_priority").asc,
        col("winner_leader").asc
      )
    val best = materialize(
      "mapping",
      pairs
        .withColumn("pick", row_number().over(bestWin))
        .filter(col("pick") === 1)
        .drop("winner_identifiers", "loser_identifiers")
    )

    val merges = best.select(
      col("pref_key").as("matched_preferred_name"),
      col("category"),
      col("taxon_scope"),
      col("winner_leader"),
      col("loser_leader").as("absorbed_leader"),
      col("winner_id_count"),
      col("loser_ids").as("absorbed_id_count"),
      col("winner_name_count"),
      col("loser_name_count").as("absorbed_name_count")
    )

    // Fold losers into winners.
    // Fetch wide payload only for proven losers, once the narrow mapping is materialized.
    val absorbed = best
      .select("loser_leader", "winner_leader", "loser_priority", "loser_source")
      .join(
        cliques.select(
          col("leader").as("loser_leader"),
          col("names").as("loser_names"),
          col("taxa").as("loser_taxa"),
          col("identifiers").as("loser_identifiers")
        ),
        Seq("loser_leader")
      )
      .groupBy("winner_leader")
      .agg(
        flatten(collect_list("loser_names")).as("absorbed_names"),
        flatten(collect_list("loser_taxa")).as("absorbed_taxa"),
        flatten(collect_list("loser_identifiers")).as("absorbed_identifiers"),
        min(col("loser_priority")).as("absorbed_priority"),
        collect_set(col("loser_source")).as("absorbed_sources")
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
        concat_ws(
          "+",
          sort_array(
            array_distinct(
              concat(array(col("source")), coalesce(col("absorbed_sources"), array().cast("array<string>")))
            )
          )
        ).as("source"),
        least(col("priority"), coalesce(col("absorbed_priority"), col("priority"))).as("priority")
      )
      // The count is derived from the unioned identifier list, never summed:
      // loser and winner lists can overlap.
      .withColumn("identifier_count", size(col("identifiers")))

    Result(surviving, merges)
  }
}
