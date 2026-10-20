package biolinkamalgam.transform

import biolinkamalgam.config.Config
import biolinkamalgam.config.Config.FilterConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/** The core rule: global synonym uniqueness scoped by (category, taxon_scope).
  *
  * A normalized name key may live in exactly ONE clique's SYNONYMS within a scope. The same name MAY recur across
  * different categories or different taxon scopes (the exception). Arbitration runs in two passes mirroring the
  * fullmap DB's two-level probing: pass 1 on the level-one (Porter2) key, pass 2 on the level-two ([a-z0-9_]-only)
  * key among the survivors (skipped when l2 is empty or equals l1 — those keys already arbitrated in pass 1).
  *
  * Admission gates run before arbitration, on fullmap's emit-path semantics (TermGates): token QC plus dead-term
  * checks on the cleaned form AND on the level-one key.
  *
  * Winner order in both passes: preferred-name match (strip-only comparison), then most synonyms, then identifier
  * count, then source priority (lower wins), then smallest leader CURIE. Losers are logged to the conflicts TSV with
  * their winner and the NLP level that eliminated them; surviving names are re-aggregated into the canonical output
  * shape, each carrying its l1 key and — fullmap dedup (`emit_term`: l2 is stored only when it differs from l1 and
  * passes the gates) — its l2 key when different, else null.
  */
object Uniqueness {

  case class Result(output: DataFrame, conflicts: DataFrame)

  def apply(cliques: DataFrame, majorTaxa: Set[String], filters: FilterConfig): Result = {
    val scopeUdf = udf { taxa: Seq[String] =>
      Config.taxonScopes(Option(taxa).getOrElse(Seq.empty), majorTaxa)
    }

    val banned = filters.bannedSubstrings
    val dead = filters.deadTerms
    val exclude = filters.compiledExcludeNameRegex
    val tokenQcUdf = udf { cleaned: String => TermGates.tokenQc(cleaned, banned) }
    val deadUdf = udf { term: String =>
      TermGates.isDeadTerm(term, dead, exclude)
    }

    val exploded = cliques
      .withColumn("name", explode(col("names")))
      .filter(length(col("name")) > 0)
      .withColumn("cleaned", udf((s: String) => NameKey.clean(s)).apply(col("name")))
      .filter(tokenQcUdf(col("cleaned")))
      .filter(!deadUdf(col("cleaned")))
      .withColumn("l1", NameKey.l1Key(col("name")))
      .filter(length(col("l1")) > 0)
      .filter(!deadUdf(col("l1")))
      .withColumn("l2", NameKey.l2Key(col("name")))
      .withColumn("taxon_scope", explode(scopeUdf(col("taxa"))))
      .withColumn("name_count", size(col("names")))
      .withColumn(
        "is_preferred",
        NameKey.stripKey(col("name")) === NameKey.stripKey(col("preferred_name"))
      )

    val rank = Seq(
      col("is_preferred").desc,
      col("name_count").desc,
      col("identifier_count").desc,
      col("priority").asc,
      col("leader").asc
    )

    def arbitrate(rows: DataFrame, keyCol: String): (DataFrame, DataFrame) = {
      val keys = Seq(keyCol, "category", "taxon_scope")
      val win = Window.partitionBy(keys.map(col): _*).orderBy(rank: _*)
      val ranked = rows.withColumn("rn", row_number().over(win))
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
          col(keyCol).as("name_key"),
          col("category"),
          col("taxon_scope"),
          col("leader").as("loser_curie"),
          col("winner_curie")
        )
        .distinct()
      (kept, conflicts)
    }

    val (keptL1, conflictsL1) = arbitrate(exploded, "l1")

    // Pass 2 only where l2 can arbitrate something pass 1 could not: non-empty
    // and different from l1 (equal keys are already unique per scope).
    val needsL2 = keptL1.filter(length(col("l2")) > 0 && col("l2") =!= col("l1"))
    val settled = keptL1.filter(!(length(col("l2")) > 0 && col("l2") =!= col("l1")))
    val (keptL2, conflictsL2) = arbitrate(needsL2, "l2")
    val kept = settled.unionByName(keptL2)

    val conflicts = conflictsL1
      .withColumn("nlp_level", lit("l1"))
      .unionByName(conflictsL2.withColumn("nlp_level", lit("l2")))

    // Re-aggregate: one row per clique leader. NAME keeps the ORIGINAL string;
    // L2 is null when it equals L1 or fails the gates (fullmap dedup), so the
    // parquet mirrors exactly what a fullmap DB build would store.
    val l2DeadUdf = udf { (l1: String, l2: String) =>
      l2.isEmpty || l2 == l1 || !TermGates.tokenQc(l2, banned) ||
      TermGates.isDeadTerm(l2, dead, exclude)
    }
    val surviving = kept
      .withColumn(
        "entry",
        struct(
          col("name").as("NAME"),
          col("l1").as("L1"),
          when(l2DeadUdf(col("l1"), col("l2")), lit(null)).otherwise(col("l2")).as("L2")
        )
      )
      .groupBy("leader")
      .agg(collect_set("entry").as("entries"))
      // sort_array cannot order structs; compare by NAME explicitly. An empty
      // string sorts first, which cannot occur (gates drop empty keys).
      .withColumn(
        "SYNONYMS",
        expr("array_sort(entries, (a, b) -> CASE WHEN a.NAME < b.NAME THEN -1 WHEN a.NAME > b.NAME THEN 1 ELSE 0 END)")
      )
      .drop("entries")

    // Cliques whose names ALL lost arbitration still get a row (empty SYNONYMS) —
    // the deliverable is one row per surviving clique, not one row per kept name.
    val output = cliques
      .select(col("leader"), col("preferred_name"), col("category"))
      .join(surviving, Seq("leader"), "left")
      .select(
        col("leader").as("CURIE"),
        col("preferred_name").as("PREFERRED_NAME"),
        col("category").as("CLASS"),
        coalesce(
          col("SYNONYMS"),
          array().cast(org.apache.spark.sql.types.ArrayType(schema.Schemas.synonymEntry, containsNull = false))
        ).as("SYNONYMS")
      )

    Result(output, conflicts)
  }
}
