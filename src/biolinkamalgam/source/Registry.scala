package biolinkamalgam.source

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/** Lists the active sources and unions their canonical clique tables.
  *
  * Adding a new source = one entry in `forConfig` — the pipeline never changes.
  */
object Registry {

  /** All sources for a run: BABEL compendia + synonyms from `babelDir` (if present), plus one CustomTableSource per
    * file in `customDir` (if present).
    */
  def forConfig(
      babelDir: Option[os.Path],
      customDir: Option[os.Path],
      promotePrefixes: Set[String]
  ): Seq[(SynonymSource, os.Path)] = {
    val babel = babelDir.toSeq.flatMap { dir =>
      Seq(
        Option.when(os.exists(dir / "compendia"))(new BabelCompendiumSource(promotePrefixes) -> dir),
        Option.when(os.exists(dir / "synonyms"))(new BabelSynonymSource() -> dir)
      ).flatten
    }
    val custom = customDir.toSeq.filter(os.exists(_)).flatMap { dir =>
      os.list(dir)
        .filter(f => f.last.endsWith(".tsv") || f.last.endsWith(".jsonl") || f.last.endsWith(".json"))
        .map(f => new CustomTableSource(f) -> dir)
    }
    babel ++ custom
  }

  /** Union of all source outputs in canonical clique shape, consolidated by leader: BABEL's compendium and synonym rows
    * for the SAME clique (and any custom source re-defining the same CURIE) fold into one row — names/taxa unioned,
    * identifier count maxed, priority minned.
    */
  def loadAll(spark: SparkSession, sources: Seq[(SynonymSource, os.Path)]): DataFrame = {
    val frames = sources.map { case (src, path) => src.load(spark, path) }
    if (frames.isEmpty)
      spark.createDataFrame(
        spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],
        biolinkamalgam.schema.Schemas.clique
      )
    else
      frames
        .reduce(_.unionByName(_))
        .groupBy("leader")
        .agg(
          first("preferred_name", ignoreNulls = true).as("preferred_name"),
          first("category", ignoreNulls = true).as("category"),
          array_distinct(flatten(collect_list("taxa"))).as("taxa"),
          array_distinct(flatten(collect_list("names"))).as("names"),
          array_distinct(flatten(collect_list("identifiers"))).as("identifiers"),
          max("identifier_count").as("identifier_count"),
          concat_ws("+", collect_set("source")).as("source"),
          min("priority").as("priority")
        )
  }
}
