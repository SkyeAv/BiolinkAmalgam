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
      StructField("identifier_count", IntegerType, nullable = false),
      StructField("source", StringType, nullable = false),
      StructField("priority", IntegerType, nullable = false)
    )
  )

  /** The deliverable: one parquet file, one row per clique leader. */
  val output: StructType = StructType(
    Seq(
      StructField("CURIE", StringType, nullable = false),
      StructField("PREFERRED_NAME", StringType, nullable = true),
      StructField("CATEGORY", StringType, nullable = false),
      StructField("SYNONYMS", ArrayType(StringType, containsNull = false), nullable = false)
    )
  )
}
