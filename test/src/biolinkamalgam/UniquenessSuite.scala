package biolinkamalgam

import biolinkamalgam.config.Config
import biolinkamalgam.source.Registry
import biolinkamalgam.transform.{CliqueMerge, NameKey, Uniqueness}
import org.apache.spark.sql.Row
import utest._

/** Synonym arbitration: one stripped name key per clique within (category, taxon scope); no stemming (downstream
  * consumers do the real NLP). Note this suite bypasses the ingest Filters (no category/allowlist drops), so the
  * fixture Publication/Agent cliques DO appear here — only PipelineSuite exercises those.
  */
object UniquenessSuite extends TestSuite {

  private val spark = TestSupport.spark

  private lazy val result: Uniqueness.Result = {
    val sources = Registry.forConfig(
      Some(TestSupport.miniSnapshot),
      Some(TestSupport.customDir),
      None,
      Set("INCHIKEY") // Isolate arbitration from wildcard identifier promotion (covered in DisplayNamesSuite).
    )
    val merged = CliqueMerge(Registry.loadAll(spark, sources), TestSupport.majorTaxa)
    val r = Uniqueness(merged.cliques, TestSupport.majorTaxa, TestSupport.filters)
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
          Seq("CURIE", "PREFERRED_NAME", "CLASS", "SYNONYMS", "TAXA")
      )
      assert(result.output.schema("SYNONYMS").dataType.sql == "ARRAY<STRING>")
      assert(result.output.schema("TAXA").dataType.sql == "ARRAY<STRING>")
    }

    test("TAXA ships the clique's valid taxa, sorted; taxonless rows get an empty list") {
      val rows = result.output.collect().map(r => r.getAs[String]("CURIE") -> r).toMap
      def taxa(curie: String) = rows(curie).getAs[scala.collection.Seq[String]]("TAXA").toList
      assert(taxa("NCBIGene:2538") == List("NCBITaxon:9606"))
      assert(taxa("NCBIGene:9003") == List("NCBITaxon:10090"))
      assert(taxa("CHEBI:17234") == List.empty)
      assert(rows.values.forall(r => !r.isNullAt(r.fieldIndex("TAXA"))))
    }

    test("conflicts log the two expected arbitrations") {
      val rows = result.conflicts
        .select("name", "category", "taxon_scope", "loser_curie", "winner_curie")
        .collect()
        .map(r => (r.getString(0), r.getString(1), r.getString(2), r.getString(3), r.getString(4)))
        .toSet
      assert(
        rows == Set(
          // PUBCHEM's "glucose" strip-collides with CHEBI's "Glucose"; CHEBI wins (preferred-name match)
          ("glucose", "biolink:SmallMolecule", Config.NoTaxon, "PUBCHEM.COMPOUND:5793", "CHEBI:17234"),
          // "shared alias" tie broken by smallest leader CURIE
          ("shared alias", "biolink:Gene", "NCBITaxon:9606", "NCBIGene:9007", "NCBIGene:9006"),
          // disjoint NCBIGene ids never merge; "rarein" is arbitrated in the shared long-tail bucket
          ("rarein", "biolink:Gene", Config.OtherTaxa, "NCBIGene:9005", "NCBIGene:9004")
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

    test("winner keeps original strings, inflections are NOT merged") {
      val rows = result.output.collect().map(r => r.getAs[String]("CURIE") -> r).toMap
      // no stemming: "glucoses" survives alongside "Glucose" — only case/punct/whitespace variants dedupe
      assert(
        synonyms(rows, "CHEBI:17234") ==
          List("D-glucose", "Glucose", "INCHIKEY:WQZGKKKJIJFFOK-GASJEMHNSA-N", "glucoses")
      )
      assert(!synonyms(rows, "NCBIGene:9007").contains("shared alias"))
      assert(synonyms(rows, "NCBIGene:9006").contains("shared alias"))
    }

    test("a clique whose only name lost still gets a row, with empty SYNONYMS") {
      val rows = result.output.collect().map(r => r.getAs[String]("CURIE") -> r).toMap
      // 19 fixture cliques - 2 merged away (same-prefix 9004/9005 are vetoed)
      assert(rows.keySet.size == 17)
      assert(synonyms(rows, "PUBCHEM.COMPOUND:5793") == List.empty)
    }

    test("admission gates drop numeric, language, stopword, and hypothetical names") {
      val rows = result.output.collect().map(r => r.getAs[String]("CURIE") -> r).toMap
      // fixture junk clique: only "genuine synonym" survives the gates
      assert(synonyms(rows, "UMLS:C7777777") == List("genuine synonym"))
    }

    test("manual entries land with priority 0 and equivalents fold into identifiers") {
      val rows = result.output.collect().map(r => r.getAs[String]("CURIE") -> r).toMap
      assert(
        synonyms(rows, "EUPATH:0009259") ==
          List("Alpha diversity", "Shannon alpha diversity", "Shannon-indexed alpha diversity data")
      )
      assert(rows("MANUAL:0000001").getAs[String]("CLASS") == "biolink:Gene")
      assert(synonyms(rows, "MANUAL:0000001") == List("manualin", "manualin alpha"))
    }
  }
}
