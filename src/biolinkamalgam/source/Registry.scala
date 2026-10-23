package biolinkamalgam.source

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Lists the active sources and unions their canonical clique tables.
  *
  * Adding a new source = one entry in `forConfig` — the pipeline never changes.
  */
object Registry {

  /** All sources for a run: BABEL compendia + synonyms from `babelDir` (if present), one CustomTableSource per file in
    * `customDir` (if present), and GTDB metadata from `gtdbDir` (if present).
    */
  def forConfig(
      babelDir: Option[os.Path],
      customDir: Option[os.Path],
      gtdbDir: Option[os.Path],
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
    // .ndjson files are hand-curated manual entries (BABEL #887 shape), priority 0 — they override.
    val manual = customDir.toSeq.filter(os.exists(_)).flatMap { dir =>
      os.list(dir)
        .filter(_.last.endsWith(".ndjson"))
        .map(f => new ManualSource(f) -> dir)
    }
    // GTDB metadata (BABEL PR #978 port): taxon cliques + NCBITaxon species crossrefs.
    val gtdb = gtdbDir.toSeq.filter(os.exists(_)).map(dir => (new GtdbSource: SynonymSource) -> dir)
    babel ++ custom ++ manual ++ gtdb
  }

  /** Union of all source outputs in canonical clique shape, consolidated by leader: BABEL's compendium and synonym rows
    * for the SAME clique (and any custom source re-defining the same CURIE) fold into one row — names/taxa unioned,
    * identifier count maxed, priority minned.
    */
  def loadAll(spark: SparkSession, sources: Seq[(SynonymSource, os.Path)], sample: Option[Long] = None): DataFrame = {
    val frames = sources.map { case (src, path) => src.load(spark, path, sample) }
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
          // Preferred name of the LOWEST-priority source wins (manual=0 beats BABEL=10), non-null preferred.
          min_by(
            col("preferred_name"),
            struct(col("priority"), col("preferred_name").isNull)
          ).as("preferred_name"),
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
