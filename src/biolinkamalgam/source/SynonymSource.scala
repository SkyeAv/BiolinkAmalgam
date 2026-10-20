package biolinkamalgam.source

import biolinkamalgam.schema.Schemas
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/** Inheritable base class for every synonym source (BABEL's or your own), modeled on BABEL's per-source datahandler
  * pattern. Adding a new source = subclass + one line in the Registry — no pipeline edits.
  *
  * `transform` must emit the canonical clique shape (Schemas.clique): (leader, preferred_name, category, taxa, names,
  * identifier_count, source, priority). Lower `priority` wins arbitration tiebreaks — give your own ontologies a lower
  * number than BABEL's to outrank it.
  */
abstract class SynonymSource(val name: String, val priority: Int) {

  /** Raw ingest from a snapshot directory. */
  def read(spark: SparkSession, path: os.Path): DataFrame

  /** Raw rows → canonical clique rows. */
  def transform(raw: DataFrame): DataFrame

  /** Optional hook before transform (e.g. filtering junk rows). */
  def preprocess(raw: DataFrame): DataFrame = raw

  /** Optional hook after transform (e.g. source-specific name cleanup). */
  def postprocess(df: DataFrame): DataFrame = df

  /** Full read pipeline; the Registry unions these. */
  final def load(spark: SparkSession, path: os.Path): DataFrame =
    postprocess(transform(preprocess(read(spark, path))))
      .withColumn("source", lit(name))
      .withColumn("priority", lit(priority))
}

/** BABEL compendia `.txt` — one JSONL clique per line; first identifier is the leader. Member labels fold into names;
  * identifiers whose prefix is in `promotePrefixes` (default INCHIKEY) also contribute their CURIE string as a name.
  */
class BabelCompendiumSource(promotePrefixes: Set[String]) extends SynonymSource("babel:compendia", priority = 10) {

  override def read(spark: SparkSession, path: os.Path): DataFrame =
    spark.read.schema(Schemas.compendium).json((path / "compendia").toString)

  override def transform(raw: DataFrame): DataFrame = {
    // `functions.transform` must be aliased: the enclosing class's own `transform`
    // method would otherwise shadow the import.
    import org.apache.spark.sql.functions.{transform => mapValues}
    val promoted = promotePrefixes.toSeq
    raw
      .filter(size(col("identifiers")) > 0)
      .select(
        col("identifiers")(0)("i").as("leader"),
        col("preferred_name"),
        col("type").as("category"),
        coalesce(col("taxa"), array()).as("taxa"),
        col("identifiers").as("identifiers")
      )
      .select(
        col("leader"),
        col("preferred_name"),
        col("category"),
        col("taxa"),
        // member labels + promoted-prefix identifier strings as names
        array_distinct(
          flatten(
            mapValues(
              col("identifiers"),
              id =>
                concat(
                  // the member's label, if any
                  when(id("l").isNotNull && length(id("l")) > 0, array(id("l")))
                    .otherwise(array()),
                  // the CURIE itself, if its prefix is promoted (INCHIKEY by default)
                  when(
                    array_contains(lit(promoted.toArray), split(id("i"), ":")(0)),
                    array(id("i"))
                  ).otherwise(array())
                )
            )
          )
        ).as("names"),
        size(col("identifiers")).as("identifier_count")
      )
  }
}

/** BABEL synonyms `.txt.gz` — one JSONL doc per clique leader. Conflated variants (synonyms-conflated/, *Conflated*)
  * are excluded BY DESIGN: drug/chemical and gene/protein cliques stay distinct, unlike Tablassert's fullmap.
  */
class BabelSynonymSource extends SynonymSource("babel:synonyms", priority = 10) {

  override def read(spark: SparkSession, path: os.Path): DataFrame = {
    val dir = (path / "synonyms").toIO
    val files =
      Option(dir.listFiles()).toSeq.flatten
        .filter(f => f.getName.endsWith(".txt.gz") || f.getName.endsWith(".txt"))
        // belt-and-braces: never read a conflated synonym file even if it lands here
        .filterNot(_.getName.contains("Conflated"))
        .map(_.getAbsolutePath)
    if (files.isEmpty) spark.emptyDataFrame
    else spark.read.schema(Schemas.synonym).json(files: _*)
  }

  override def transform(raw: DataFrame): DataFrame =
    raw
      .filter(col("curie").isNotNull)
      .select(
        col("curie").as("leader"),
        col("preferred_name"),
        // types is narrowest-first; first element is the category
        element_at(col("types"), 1).as("category"),
        coalesce(col("taxa"), array()).as("taxa"),
        coalesce(col("names"), array()).as("names"),
        coalesce(col("clique_identifier_count"), lit(1)).as("identifier_count")
      )
      .filter(col("category").isNotNull)
}

/** Example custom ontology source: TSV or JSONL files with curie, label, synonyms, category, taxa. TSV columns are
  * tab-separated with pipe-separated synonyms/taxa lists; JSONL rows match Schemas.custom. Custom sources outrank BABEL
  * by default (priority 0 < 10).
  */
class CustomTableSource(file: os.Path) extends SynonymSource(s"custom:${file.last}", priority = 0) {

  override def read(spark: SparkSession, path: os.Path): DataFrame =
    if (file.last.endsWith(".jsonl") || file.last.endsWith(".json"))
      spark.read.schema(Schemas.custom).json(file.toString)
    else
      spark.read
        .option("sep", "\t")
        .option("header", "true")
        .csv(file.toString)
        .select(
          col("curie"),
          col("label"),
          split(coalesce(col("synonyms"), lit("")), "\\|").as("synonyms"),
          col("category"),
          filter(split(coalesce(col("taxa"), lit("")), "\\|"), t => length(t) > 0)
            .as("taxa")
        )

  override def transform(raw: DataFrame): DataFrame =
    raw
      .filter(col("curie").isNotNull && col("category").isNotNull)
      .select(
        col("curie").as("leader"),
        col("label").as("preferred_name"),
        col("category"),
        coalesce(col("taxa"), array()).as("taxa"),
        array_distinct(
          filter(
            concat(
              coalesce(col("synonyms"), array()),
              when(col("label").isNotNull && length(col("label")) > 0, array(col("label")))
                .otherwise(array())
            ),
            n => length(n) > 0
          )
        ).as("names"),
        lit(1).as("identifier_count")
      )
}
