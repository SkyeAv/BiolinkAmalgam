package amalgam

import amalgam.source.Registry
import amalgam.transform.CliqueMerge
import org.apache.spark.sql.Row
import utest._

/** Clique merging: preferred-name string equality scoped by (category, taxon bucket). */
object CliqueMergeSuite extends TestSuite {

  private val spark = TestSupport.spark

  private lazy val result: CliqueMerge.Result = {
    val sources = Registry.forConfig(
      Some(TestSupport.miniSnapshot),
      Some(TestSupport.customDir),
      TestSupport.promotePrefixes
    )
    val cliques = Registry.loadAll(spark, sources)
    val r = CliqueMerge(cliques, TestSupport.majorTaxa)
    r.cliques.cache()
    r.merges.cache()
    r
  }

  private def names(row: Row): List[String] =
    row.getAs[Seq[String]]("names").toList

  val tests = Tests {

    test("same preferred name + category + taxon scope merges") {
      val pairs = result.merges
        .select("winner_leader", "absorbed_leader")
        .collect()
        .map(r => r.getString(0) -> r.getString(1))
        .toSet
      assert(
        pairs == Set(
          ("NCBIGene:9001", "HGNC:9002"), // " examplein" strips to "examplein"
          ("NCBIGene:9001", "CUSTOM:2"), // custom source's duplicate clique folds in too
          ("NCBIGene:9004", "NCBIGene:9005") // long-tail taxa share the other-taxa bucket
        )
      )
    }

    test("winner absorbs loser names, taxa, and identifier counts") {
      val winner = result.cliques.filter("leader = 'NCBIGene:9001'").head()
      assert(names(winner).contains("examplein"))
      assert(winner.getAs[Int]("identifier_count") == 4) // 2 + 1 (HGNC) + 1 (CUSTOM:2)

      val rarein = result.cliques.filter("leader = 'NCBIGene:9004'").head()
      assert(names(rarein).toSet == Set("rarein", "rarein alpha"))
      assert(
        rarein.getAs[Seq[String]]("taxa").toSet ==
          Set("NCBITaxon:1111111", "NCBITaxon:2222222")
      )
    }

    test("different taxon scope does not merge") {
      assert(result.cliques.filter("leader = 'NCBIGene:9003'").count() == 1)
    }

    test("preferred-name merge is strip-only, never stemmed") {
      // CHEBI preferred "glucose" vs PUBCHEM preferred "D-glucose": same stem family
      // but not the same written name — both cliques survive.
      assert(result.cliques.filter("leader = 'CHEBI:17234'").count() == 1)
      assert(result.cliques.filter("leader = 'PUBCHEM.COMPOUND:5793'").count() == 1)
    }

    test("cross-category preferred-name equality does not merge") {
      // DRUGBANK (biolink:Drug) preferred "glucose" == CHEBI (SmallMolecule) preferred —
      // different category, no merge.
      assert(result.cliques.filter("leader = 'DRUGBANK:DB00001'").count() == 1)
    }

    test("empty preferred names never merge and survive") {
      assert(result.cliques.filter("leader = 'UMLS:C9999999'").count() == 1)
      assert(result.cliques.count() == 11)
    }
  }
}
