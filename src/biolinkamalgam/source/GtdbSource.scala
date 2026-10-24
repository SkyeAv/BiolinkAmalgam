package biolinkamalgam.source

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/** GTDB (Genome Taxonomy Database) metadata as a `biolink:OrganismTaxon` source — the BiolinkAmalgam port of BABEL
  * PR #978. Input: one or more GTDB `bac120`/`ar53` metadata TSVs (plain or .gz), read by column NAME:
  * `gtdb_taxonomy` ("d__Bacteria; p__Pseudomonadota; ...; s__Escherichia coli") and `ncbi_species_taxid`.
  *
  * Every rank token becomes a GTDB clique: CURIE `GTDB:s__Escherichia_coli` (spaces to underscores, Bioregistry
  * form), preferred name the display label ("Escherichia coli"). Species carrying an `ncbi_species_taxid` crossref
  * into the biolink taxon space: the GTDB CURIE becomes an identifier of the `NCBITaxon:<taxid>` clique (folding in
  * via the Registry's by-leader union) AND a `GTDB:`-prefixed synonym of it, since the export schema carries no
  * identifiers column and the crossref must be visible in the parquet (same mechanism as INCHIKEY promotion).
  *
  * Many-to-one guard (PR #978 parity): `ncbi_species_taxid` is a PER-GENOME column and GTDB disagrees with NCBI
  * constantly, so a species' crossref is its MAJORITY taxid (most genomes; ties -> smallest taxid), and several
  * species can still majority-map to one taxid. Those must never merge: the species with the most genomes backing
  * that taxid wins the crossref, the rest stay GTDB singletons. All tiebreaks deterministic (support desc, then
  * CURIE asc) so reruns reproduce.
  */
class GtdbSource extends SynonymSource("gtdb", priority = 10) {

  override def read(spark: SparkSession, path: os.Path): DataFrame = {
    val files = os.list(path).filter { f =>
      os.isFile(f) && (f.last.endsWith(".tsv") || f.last.endsWith(".tsv.gz"))
    }
    if (files.isEmpty) throw new IllegalArgumentException(s"no GTDB metadata `.tsv`/`.tsv.gz` files in $path")
    spark.read
      .option("sep", "\t")
      .option("header", "true")
      .csv(files.map(_.toString): _*)
      .select(col("gtdb_taxonomy"), col("ncbi_species_taxid"))
  }

  override def transform(raw: DataFrame): DataFrame = {
    // "s__Escherichia coli" -> rank "s", token kept as written (display decodes underscores to spaces).
    val labeled = raw
      .select(explode(split(col("gtdb_taxonomy"), ";")).as("token"), col("ncbi_species_taxid"))
      .withColumn("token", trim(col("token")))
      .filter(col("token").rlike("^[a-z]__\\S.*$"))
      .withColumn("rank", substring_index(col("token"), "__", 1))
      .withColumn("curie", concat(lit("GTDB:"), regexp_replace(col("token"), " ", "_")))
      .withColumn("display", regexp_replace(substring_index(col("token"), "__", -1), "_", " "))

    // Per-genome (species, taxid) pairs -> support counts; each species crossrefs its majority taxid.
    val pairs = labeled
      .filter(col("rank") === "s")
      .select("curie", "display", "ncbi_species_taxid")
      .withColumn("taxid", firstTaxid(col("ncbi_species_taxid")))
      .filter(col("taxid").isNotNull)
      .groupBy("curie", "display", "taxid")
      .agg(count(lit(1)).as("genomes"))

    val speciesWin = Window.partitionBy(col("curie")).orderBy(col("genomes").desc, col("taxid").asc)
    val resolved = pairs
      .withColumn("rn", row_number().over(speciesWin))
      .filter(col("rn") === 1)
      .select("curie", "display", "taxid", "genomes")

    val taxidWin = Window.partitionBy(col("taxid")).orderBy(col("genomes").desc, col("curie").asc)
    val ranked = resolved.withColumn("rn2", row_number().over(taxidWin))
    val crossref = ranked.filter(col("rn2") === 1)
    val taxidSingletons = ranked.filter(col("rn2") > 1).select("curie", "display")
    val noTaxid = labeled
      .filter(col("rank") === "s")
      .select("curie", "display")
      .distinct
      .join(pairs.select("curie").distinct, Seq("curie"), "left_anti")
    val higherRanks = labeled.filter(col("rank") =!= "s").select("curie", "display").distinct

    val singletonTaxa = taxidSingletons.unionByName(noTaxid).unionByName(higherRanks)

    singletonTaxa
      .select(
        col("curie").as("leader"),
        col("display").as("preferred_name"),
        lit("biolink:OrganismTaxon").as("category"),
        array().cast("array<string>").as("taxa"),
        array(col("display")).as("names"),
        array(col("curie")).as("identifiers"),
        lit(1).as("identifier_count")
      )
      .unionByName(
        crossref.select(
          concat(lit("NCBITaxon:"), col("taxid")).as("leader"),
          col("display").as("preferred_name"),
          lit("biolink:OrganismTaxon").as("category"),
          array().cast("array<string>").as("taxa"),
          array(col("display"), col("curie")).as("names"),
          array(col("curie")).as("identifiers"),
          lit(1).as("identifier_count")
        )
      )
  }

  /** First positive integer in a taxid cell ("562" / "562,570" / "NA" / null). None when the cell carries no valid
    * crossref — those species stay GTDB singletons.
    */
  private val firstTaxid = udf { s: String =>
    if (s == null) None
    else
      s.split("[,;]")
        .map(_.trim)
        .find(t => t.nonEmpty && t.forall(_.isDigit))
        .map(_.toLong)
        .filter(_ > 0)
  }
}
