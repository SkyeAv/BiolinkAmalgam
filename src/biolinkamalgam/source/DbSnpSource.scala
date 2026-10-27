package biolinkamalgam.source

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Full RefSNP JSON projection. One identity per rsID, independent of allele/assembly placements. Retain every
  * lifecycle partition; merged and withdrawn identities remain history, not live synonym rows.
  */
object DbSnpSource {
  private val strings = ArrayType(StringType, containsNull = true)
  private def struct(fields: (String, DataType)*): StructType =
    StructType(fields.map { case (name, kind) => StructField(name, kind) })
  private def array(kind: DataType): ArrayType = ArrayType(kind, containsNull = true)
  private val gene = struct("id" -> StringType)
  private val annotation = struct("assembly_annotation" -> array(struct("genes" -> array(gene))))

  /** Partial schema deliberately excludes huge placements, frequencies and RNA/allele annotation payloads. */
  val schema: StructType = struct(
    "refsnp_id" -> StringType,
    "last_update_build_id" -> StringType,
    "citations" -> strings,
    "primary_snapshot_data" -> struct("variant_type" -> StringType, "allele_annotations" -> array(annotation)),
    "merged_snapshot_data" -> struct("merged_into" -> strings),
    "withdrawn_snapshot_data" -> struct("withdrawn_time" -> StringType),
    "unsupported_snapshot_data" -> struct("alleles_from_previous_support" -> array(struct("seq_id" -> StringType))),
    "nosnppos_snapshot_data" -> struct("support" -> array(struct("revision_added" -> StringType)))
  )

  def read(spark: SparkSession, path: os.Path): DataFrame =
    spark.read.schema(schema).option("mode", "FAILFAST").json(path.toString)

  /** Project only identifiers, lifecycle status and typed association targets; no preferred gene/allele strings. */
  def project(raw: DataFrame): DataFrame = {
    val snapshots = Seq("primary", "merged", "withdrawn", "unsupported", "nosnppos")
    val present = snapshots.map(s => col(s"${s}_snapshot_data").isNotNull.cast("int"))
    val status = snapshots.foldRight(lit(null).cast("string")) { (key, fallback) =>
      when(
        col(s"${key}_snapshot_data").isNotNull,
        lit(if (key == "primary") "active" else if (key == "nosnppos") "locationless" else key)
      ).otherwise(fallback)
    }
    val emptyStrings = org.apache.spark.sql.functions.array().cast("array<string>")
    raw.select(
      col("refsnp_id").as("rsid"),
      status.as("status"),
      present.reduce(_ + _).as("snapshot_count"),
      col("last_update_build_id").as("last_update_build"),
      sort_array(
        array_distinct(
          coalesce(
            expr("""
        flatten(transform(primary_snapshot_data.allele_annotations, a ->
          coalesce(flatten(transform(a.assembly_annotation, b -> coalesce(transform(b.genes, g -> g.id), array()))), array())))
      """),
            emptyStrings
          )
        )
      ).as("genes"),
      sort_array(array_distinct(coalesce(col("citations"), emptyStrings))).as("citations"),
      sort_array(array_distinct(coalesce(col("merged_snapshot_data.merged_into"), emptyStrings))).as("merged_into")
    )
  }

  /** Validate each completed shard; cross-shard uniqueness and release counts are a separate completion gate. */
  def validate(projected: DataFrame): Unit = {
    val invalidTargets =
      udf((ids: Seq[String]) => ids == null || ids.exists(i => i == null || !i.matches("[1-9][0-9]*")))
    val invalid = projected.filter(
      col("rsid").isNull || !col("rsid").rlike("^[1-9][0-9]*$") || col("snapshot_count") =!= 1 ||
        col("status").isNull || !col("status").isin("active", "merged", "withdrawn", "unsupported", "locationless") ||
        invalidTargets(col("genes")) || invalidTargets(col("citations")) || invalidTargets(col("merged_into")) ||
        (col("status") === "merged" && size(col("merged_into")) === 0) || array_contains(
          col("merged_into"),
          col("rsid")
        )
    )
    require(invalid.limit(1).count() == 0, "invalid RefSNP id, snapshot lifecycle or crossref target")
    require(
      projected.groupBy("rsid").count().filter(col("count") > 1).limit(1).count() == 0,
      "duplicate RefSNP ids in shard"
    )
  }

  /** Identity-only names cannot collide: these rows bypass lexical ranking and all-pairs synonym operations. */
  def output(projected: DataFrame): DataFrame = projected
    .filter(col("status").isin("active", "unsupported", "locationless"))
    .select(
      concat(lit("DBSNP:rs"), col("rsid")).as("CURIE"),
      concat(lit("rs"), col("rsid")).as("PREFERRED_NAME"),
      lit("biolink:SequenceVariant").as("CLASS"),
      org.apache.spark.sql.functions
        .array(concat(lit("rs"), col("rsid")), concat(lit("DBSNP:rs"), col("rsid")))
        .as("SYNONYMS")
    )

  def crossrefs(projected: DataFrame): DataFrame = {
    def relation(field: String, predicate: String, prefix: String): DataFrame = projected
      .select(concat(lit("DBSNP:rs"), col("rsid")).as("subject"), explode(col(field)).as("target"))
      .select(
        col("subject"),
        lit(predicate).as("predicate"),
        concat(lit(prefix), col("target")).as("object"),
        lit("dbsnp").as("source")
      )
    relation("genes", "gene_association", "NCBIGene:")
      .unionByName(relation("citations", "cites", "PMID:"))
      .unionByName(relation("merged_into", "merged_into", "DBSNP:rs"))
  }
}
