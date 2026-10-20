package biolinkamalgam

import biolinkamalgam.config.Config
import biolinkamalgam.source.Registry
import org.apache.spark.sql.{DataFrame, Row}
import utest._

/** Source ingest against the mini fixture snapshot: BABEL parsing, conflation exclusion, InChIKey promotion,
  * custom-ontology TSVs, and leader consolidation.
  */
object SourceSuite extends TestSuite {

  private val spark = TestSupport.spark

  private lazy val cliques: DataFrame = {
    val sources = Registry.forConfig(
      Some(TestSupport.miniSnapshot),
      Some(TestSupport.customDir),
      TestSupport.promotePrefixes
    )
    Registry.loadAll(spark, sources).cache()
  }

  private def byLeader(leader: String): Row =
    cliques.filter("leader = '" + leader + "'").head()

  private def names(row: Row): List[String] =
    row.getAs[Seq[String]]("names").toList

  val tests = Tests {

    test("taxon bucketing: major keeps scope, long-tail shares, none shares") {
      val major = TestSupport.majorTaxa
      assert(Config.taxonScopes(Seq("NCBITaxon:9606"), major) == Seq("NCBITaxon:9606"))
      assert(Config.taxonScopes(Seq("NCBITaxon:1111111"), major) == Seq(Config.OtherTaxa))
      assert(Config.taxonScopes(Seq.empty, major) == Seq(Config.NoTaxon))
    }

    test("all fixture cliques load, one row per leader") {
      // 14 compendium cliques + 1 synonyms-only clique (UMLS:C7777777 gates fixture)
      assert(cliques.count() == 15)
      assert(cliques.select("leader").distinct().count() == 15)
    }

    test("verbatim BABEL G6PC1 compendium line parses") {
      val row = byLeader("NCBIGene:2538")
      assert(row.getAs[String]("preferred_name") == "G6PC1")
      assert(row.getAs[String]("category") == "biolink:Gene")
      assert(row.getAs[Int]("identifier_count") == 5)
      // collect_list order is not guaranteed; compare as sets
      assert(row.getAs[Seq[String]]("identifiers").toSet == Set(
        "NCBIGene:2538", "ENSEMBL:ENSG00000131482", "HGNC:4056", "OMIM:613742", "UMLS:C1414892"
      ))
      assert(row.getAs[Seq[String]]("taxa").toList == List("NCBITaxon:9606"))
      val ns = names(row)
      assert(ns.contains("G6PC1"))
      assert(ns.contains("G6PC1 (Hsap)"))
      assert(ns.contains("G6PC1 gene"))
      assert(ns.contains("glucose-6-phosphatase catalytic subunit 1"))
    }

    test("compendium + synonyms rows for one clique consolidate by leader") {
      val row = byLeader("NCBIGene:9001")
      val src = row.getAs[String]("source")
      assert(src.contains("babel:compendia") && src.contains("babel:synonyms"))
      val ns = names(row)
      assert(ns.contains("examplein-1")) // from synonyms file
      assert(ns.contains("EXAMPLEIN")) // from compendium member label
    }

    test("conflated synonym files are never read") {
      assert(cliques.filter("leader = 'CONFLATED:1'").count() == 0)
      val allNames = cliques
        .select(org.apache.spark.sql.functions.explode(org.apache.spark.sql.functions.col("names")))
        .distinct()
        .collect()
        .map(_.getString(0))
      assert(!allNames.contains("should not appear"))
    }

    test("INCHIKEY identifiers are promoted to synonyms") {
      val ns = names(byLeader("CHEBI:17234"))
      assert(ns.contains("INCHIKEY:WQZGKKKJIJFFOK-GASJEMHNSA-N"))
      assert(ns.contains("glucoses"))
    }

    test("custom TSV ontology loads and outranks BABEL") {
      val custom = byLeader("CUSTOM:1")
      assert(custom.getAs[Int]("priority") == 0)
      assert(custom.getAs[String]("preferred_name") == "Customin")
      val ns = names(custom)
      assert(ns.contains("customin alpha"))
      assert(byLeader("CUSTOM:2").getAs[Int]("priority") == 0)
    }

    test("null preferred name stays null") {
      val row = byLeader("UMLS:C9999999")
      assert(row.getAs[String]("preferred_name") == null)
      assert(names(row) == List("nameless"))
    }

    test("synonyms-only cliques list just the leader as identifiers") {
      val row = byLeader("UMLS:C7777777")
      assert(row.getAs[Seq[String]]("identifiers").toList == List("UMLS:C7777777"))
      assert(row.getAs[Int]("identifier_count") == 1)
    }
  }
}
