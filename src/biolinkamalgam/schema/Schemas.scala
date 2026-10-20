package biolinkamalgam.schema

import org.apache.spark.sql.types._

/** Explicit StructTypes for every file shape BiolinkAmalgam reads or writes.
  *
  * Never rely on Spark schema inference: on 30-40GB of gzipped JSONL it costs a full extra pass and can silently change
  * between snapshots. Every reader must use these.
  */
object Schemas {

  /** One identifier entry inside a BABEL compendium line: i=CURIE, l=label, t=taxa. */
  val identifier: StructType = StructType(
    Seq(
      StructField("i", StringType, nullable = true),
      StructField("l", StringType, nullable = true),
      StructField("t", ArrayType(StringType, containsNull = false), nullable = true)
    )
  )

  /** A BABEL compendia `.txt` JSONL line (one clique). First identifier is the leader. */
  val compendium: StructType = StructType(
    Seq(
      StructField("type", StringType, nullable = false),
      StructField("identifiers", ArrayType(identifier, containsNull = false), nullable = false),
      StructField("preferred_name", StringType, nullable = true),
      StructField("taxa", ArrayType(StringType, containsNull = false), nullable = true)
    )
  )

  /** A BABEL synonyms `.txt.gz` JSONL line (one clique leader). `types` is narrowest-first. */
  val synonym: StructType = StructType(
    Seq(
      StructField("curie", StringType, nullable = false),
      StructField("names", ArrayType(StringType, containsNull = false), nullable = true),
      StructField("types", ArrayType(StringType, containsNull = false), nullable = true),
      StructField("preferred_name", StringType, nullable = true),
      StructField("taxa", ArrayType(StringType, containsNull = false), nullable = true),
      StructField("clique_identifier_count", IntegerType, nullable = true)
    )
  )

  /** A custom-ontology JSONL row (TSV variant is parsed with the same target shape). */
  val custom: StructType = StructType(
    Seq(
      StructField("curie", StringType, nullable = false),
      StructField("label", StringType, nullable = true),
      StructField("synonyms", ArrayType(StringType, containsNull = false), nullable = true),
      StructField("category", StringType, nullable = false),
      StructField("taxa", ArrayType(StringType, containsNull = false), nullable = true)
    )
  )

  /** Canonical clique table flowing through merge and arbitration. */
  val clique: StructType = StructType(
    Seq(
      StructField("leader", StringType, nullable = false),
      StructField("preferred_name", StringType, nullable = true),
      StructField("category", StringType, nullable = false),
      StructField("taxa", ArrayType(StringType, containsNull = false), nullable = false),
      StructField("names", ArrayType(StringType, containsNull = false), nullable = false),
      // Every identifier CURIE in the clique (leader first for compendia
      // sources). Needed for prefix-scoped filtering; identifier_count is the
      // authoritative count (BABEL synonyms rows carry clique_identifier_count
      // even though they list only the leader).
      StructField("identifiers", ArrayType(StringType, containsNull = false), nullable = false),
      StructField("identifier_count", IntegerType, nullable = false),
      StructField("source", StringType, nullable = false),
      StructField("priority", IntegerType, nullable = false)
    )
  )

  /** One surviving synonym with its fullmap keys. NAME keeps the original string; L1 is the level-one (Porter2)
    * key; L2 is the level-two ([a-z0-9_]-only) key, null when it equals L1 or fails the gates — the exact dedup the
    * fullmap DB build applies at emit time, so the parquet converts into the DB with no re-normalization.
    */
  val synonymEntry: StructType = StructType(
    Seq(
      StructField("NAME", StringType, nullable = false),
      StructField("L1", StringType, nullable = false),
      StructField("L2", StringType, nullable = true)
    )
  )

  /** The deliverable: one parquet file, one row per clique leader. */
  val output: StructType = StructType(
    Seq(
      StructField("CURIE", StringType, nullable = false),
      StructField("PREFERRED_NAME", StringType, nullable = true),
      StructField("CLASS", StringType, nullable = false),
      StructField("SYNONYMS", ArrayType(synonymEntry, containsNull = false), nullable = false)
    )
  )
}
