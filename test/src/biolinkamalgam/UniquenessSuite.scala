package biolinkamalgam

import biolinkamalgam.config.Config
import biolinkamalgam.source.Registry
import biolinkamalgam.transform.{CliqueMerge, Uniqueness}
import org.apache.spark.sql.Row
import utest._

/** Synonym arbitration: one stemmed name key per clique within (category, taxon scope). */
object UniquenessSuite extends TestSuite {

  private val spark = TestSupport.spark

  private lazy val result: Uniqueness.Result = {
    val sources = Registry.forConfig(
      Some(TestSupport.miniSnapshot),
      Some(TestSupport.customDir),
      TestSupport.promotePrefixes
    )
    val merged = CliqueMerge(Registry.loadAll(spark, sources), TestSupport.majorTaxa)
    val r = Uniqueness(merged.cliques, TestSupport.majorTaxa)
    r.output.cache()
    r.conflicts.cache()
    r
  }

  private def synonyms(rows: Map[String, Row], curie: String): List[String] =
    rows(curie).getAs[Seq[String]]("SYNONYMS").toList

  val tests = Tests {

    test("output has exactly the canonical schema") {
      assert(
        result.output.schema.fieldNames.toSeq ==
          Seq("CURIE", "PREFERRED_NAME", "CATEGORY", "SYNONYMS")
      )
    }

    test("conflicts log the two expected arbitrations") {
      val rows = result.conflicts
        .select("name", "category", "taxon_scope", "loser_curie", "winner_curie")
        .collect()
        .map(r => (r.getString(0), r.getString(1), r.getString(2), r.getString(3), r.getString(4)))
        .toSet
      assert(
        rows == Set(
          // "glucose" and "glucoses" stem-collide; CHEBI wins on name count
          ("glucose", "biolink:SmallMolecule", Config.NoTaxon, "PUBCHEM.COMPOUND:5793", "CHEBI:17234"),
          // "shared alias" tie broken by smallest leader CURIE
          ("shared alias", "biolink:Gene", "NCBITaxon:9606", "NCBIGene:9007", "NCBIGene:9006")
        )
      )
    }

    test("same name may recur across categories (the exception)") {
      val rows = result.output.collect().map(r => r.getAs[String]("CURIE") -> r).toMap
      assert(synonyms(rows, "DRUGBANK:DB00001") == List("glucose"))
      assert(synonyms(rows, "CHEBI:17234").contains("Glucose"))
    }

    test("same name may recur across taxon scopes") {
      val rows = result.output.collect().map(r => r.getAs[String]("CURIE") -> r).toMap
      val mouse = synonyms(rows, "NCBIGene:9003").map(stripKey)
      val human = synonyms(rows, "NCBIGene:9001").map(stripKey)
      assert(mouse.contains("examplein"))
      assert(human.contains("examplein"))
    }

    test("winner keeps original strings, loser keeps nothing of the contested name") {
      val rows = result.output.collect().map(r => r.getAs[String]("CURIE") -> r).toMap
      val chebi = synonyms(rows, "CHEBI:17234")
      assert(chebi == List("D-glucose", "Glucose", "INCHIKEY:WQZGKKKJIJFFOK-GASJEMHNSA-N"))
      assert(!synonyms(rows, "NCBIGene:9007").contains("shared alias"))
      assert(synonyms(rows, "NCBIGene:9006").contains("shared alias"))
    }

    test("a clique whose only name lost still gets a row, with empty SYNONYMS") {
      val rows = result.output.collect().map(r => r.getAs[String]("CURIE") -> r).toMap
      assert(rows.keySet.size == 11)
      assert(synonyms(rows, "PUBCHEM.COMPOUND:5793") == List.empty)
    }
  }

  private def stripKey(s: String): String = transform.NameKey.strip(s)
}
