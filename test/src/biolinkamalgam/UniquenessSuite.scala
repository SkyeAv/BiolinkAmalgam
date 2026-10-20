package biolinkamalgam

import biolinkamalgam.config.Config
import biolinkamalgam.source.Registry
import biolinkamalgam.transform.{CliqueMerge, NameKey, Uniqueness}
import org.apache.spark.sql.Row
import utest._

/** Synonym arbitration: one normalized name key per clique within (category, taxon scope), two passes (l1 then l2),
  * admission gates on. Note this suite bypasses the ingest Filters (no category/allowlist drops), so the fixture
  * Publication/Agent cliques DO appear here — only PipelineSuite exercises those.
  */
object UniquenessSuite extends TestSuite {

  private val spark = TestSupport.spark

  private lazy val result: Uniqueness.Result = {
    val sources = Registry.forConfig(
      Some(TestSupport.miniSnapshot),
      Some(TestSupport.customDir),
      TestSupport.promotePrefixes
    )
    val merged = CliqueMerge(Registry.loadAll(spark, sources), TestSupport.majorTaxa)
    val r = Uniqueness(merged.cliques, TestSupport.majorTaxa, TestSupport.filters)
    r.output.cache()
    r.conflicts.cache()
    r
  }

  private def entries(rows: Map[String, Row], curie: String): List[Row] =
    rows(curie).getAs[Seq[Row]]("SYNONYMS").toList

  private def synonyms(rows: Map[String, Row], curie: String): List[String] =
    entries(rows, curie).map(_.getAs[String]("NAME"))

  val tests = Tests {

    test("output has exactly the canonical schema") {
      assert(
        result.output.schema.fieldNames.toSeq ==
          Seq("CURIE", "PREFERRED_NAME", "CLASS", "SYNONYMS")
      )
      val entry = result.output.schema("SYNONYMS").dataType
        .asInstanceOf[org.apache.spark.sql.types.ArrayType]
        .elementType
        .asInstanceOf[org.apache.spark.sql.types.StructType]
      assert(entry.fieldNames.toSeq == Seq("NAME", "L1", "L2"))
    }

    test("conflicts log the two expected arbitrations, both at level l1") {
      val rows = result.conflicts
        .select("name", "category", "taxon_scope", "loser_curie", "winner_curie", "nlp_level")
        .collect()
        .map(r => (r.getString(0), r.getString(1), r.getString(2), r.getString(3), r.getString(4), r.getString(5)))
        .toSet
      assert(
        rows == Set(
          // "glucose" and "glucoses" l1-collide; CHEBI wins (preferred-name match)
          ("glucose", "biolink:SmallMolecule", Config.NoTaxon, "PUBCHEM.COMPOUND:5793", "CHEBI:17234", "l1"),
          // "shared alias" tie broken by smallest leader CURIE
          ("shared alias", "biolink:Gene", "NCBITaxon:9606", "NCBIGene:9007", "NCBIGene:9006", "l1")
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
      val mouse = synonyms(rows, "NCBIGene:9003").map(NameKey.strip)
      val human = synonyms(rows, "NCBIGene:9001").map(NameKey.strip)
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
      // 11 pre-existing + fixture Publication/Agent cliques + the junk-gate clique
      // (this suite bypasses ingest filters)
      assert(rows.keySet.size == 14)
      assert(synonyms(rows, "PUBCHEM.COMPOUND:5793") == List.empty)
    }

    test("admission gates drop numeric, language, stopword, and hypothetical names") {
      val rows = result.output.collect().map(r => r.getAs[String]("CURIE") -> r).toMap
      // fixture junk clique: only "genuine synonym" survives the gates
      assert(synonyms(rows, "UMLS:C7777777") == List("genuine synonym"))
    }

    test("every surviving synonym carries its l1 key; l2 present only when it differs (fullmap dedup)") {
      val rows = result.output.collect().map(r => r.getAs[String]("CURIE") -> r).toMap
      val g6pc1 = entries(rows, "NCBIGene:2538").map(r => r.getAs[String]("NAME") -> r).toMap
      // atomic token: l1 == l2 -> L2 nulled (exactly what fullmap's emit_term stores)
      assert(g6pc1("G6PC1").getAs[String]("L1") == "g6pc1")
      assert(g6pc1("G6PC1").getAs[String]("L2") == null)
      // stemmable token: l2 keeps the unstemmed form, so it is stored
      val chebi = entries(rows, "CHEBI:17234").map(r => r.getAs[String]("NAME") -> r).toMap
      assert(chebi("Glucose").getAs[String]("L1") == NameKey.l1("Glucose"))
      assert(chebi("Glucose").getAs[String]("L2") == "glucose")
      // invariants across every entry in the dataset
      result.output.collect().foreach { row =>
        row.getAs[Seq[Row]]("SYNONYMS").foreach { e =>
          val name = e.getAs[String]("NAME")
          assert(e.getAs[String]("L1") == NameKey.l1(name))
          val l2 = e.getAs[String]("L2")
          assert(l2 == null || (l2 == NameKey.l2(name) && l2 != e.getAs[String]("L1")))
        }
      }
    }
  }
}
