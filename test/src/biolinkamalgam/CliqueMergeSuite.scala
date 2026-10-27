package biolinkamalgam

import biolinkamalgam.source.Registry
import biolinkamalgam.transform.CliqueMerge
import org.apache.spark.sql.Row
import utest._

/** Clique merging: preferred-name string equality scoped by (category, taxon bucket). */
object CliqueMergeSuite extends TestSuite {

  private val spark = TestSupport.spark

  private lazy val result: CliqueMerge.Result = {
    val sources = Registry.forConfig(
      Some(TestSupport.miniSnapshot),
      Some(TestSupport.customDir),
      None,
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
          ("NCBIGene:9001", "CUSTOM:2") // custom source's duplicate clique folds in too
          // NCBIGene:9004/9005 share "rarein" and the other-taxa bucket but are two NCBIGene ids:
          // the namespace veto keeps two species' genes apart.
        )
      )
    }

    test("winner absorbs loser names, taxa, and identifiers") {
      val winner = result.cliques.filter("leader = 'NCBIGene:9001'").head()
      assert(names(winner).contains("examplein"))
      // union of [NCBIGene:9001, ENSEMBL:ENSG00000999999] + [HGNC:9002] + [CUSTOM:2]
      assert(winner.getAs[Int]("identifier_count") == 4)
      assert(
        winner.getAs[Seq[String]]("identifiers").toSet ==
          Set("NCBIGene:9001", "ENSEMBL:ENSG00000999999", "HGNC:9002", "CUSTOM:2")
      )

    }

    test("namespace veto: same name, same prefix, disjoint identifiers never merge") {
      assert(result.cliques.filter("leader = 'NCBIGene:9004'").count() == 1)
      assert(result.cliques.filter("leader = 'NCBIGene:9005'").count() == 1)
      assert(CliqueMerge.namespaceConflict(Seq("PUBCHEM.COMPOUND:1", "CAS:1"), Seq("PUBCHEM.COMPOUND:2")))
      assert(!CliqueMerge.namespaceConflict(Seq("NCBIGene:1", "HGNC:2"), Seq("HGNC:2", "UMLS:C1")))
      assert(!CliqueMerge.namespaceConflict(Seq("NCBIGene:1"), Seq("HGNC:2"))) // disjoint prefixes merge
      assert(!CliqueMerge.namespaceConflict(null, Seq("HGNC:2")))
      assert(CliqueMerge.groupNamespaceConflict(Seq(Seq("X:1", "X:2"), Seq("X:1"), Seq("X:2"))))
      assert(!CliqueMerge.groupNamespaceConflict(Seq(Seq("X:1", "X:2"), Seq("X:1"), Seq("X:1", "Y:2"))))
      // a third clique cannot pick a side in an ambiguous group: nothing in the group merges
      import spark.implicits._
      val df = Seq(
        ("PUBCHEM.COMPOUND:1", "Cobalt;erbium", Seq("PUBCHEM.COMPOUND:1", "x"), Seq("PUBCHEM.COMPOUND:1")),
        ("PUBCHEM.COMPOUND:2", "Cobalt;erbium", Seq("y"), Seq("PUBCHEM.COMPOUND:2")),
        ("MESH:C1", "Cobalt;erbium", Seq("z"), Seq("MESH:C1"))
      ).toDF("leader", "preferred_name", "names", "identifiers")
        .withColumn("category", org.apache.spark.sql.functions.lit("biolink:MolecularMixture"))
        .withColumn("taxa", org.apache.spark.sql.functions.typedLit(Seq.empty[String]))
        .withColumn("identifier_count", org.apache.spark.sql.functions.size($"identifiers"))
        .withColumn("source", org.apache.spark.sql.functions.lit("babel"))
        .withColumn("priority", org.apache.spark.sql.functions.lit(10))
      val r = CliqueMerge(df, Set.empty)
      assert(r.cliques.count() == 3)
      assert(r.merges.count() == 0)
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
      assert(result.cliques.count() == 17) // 19 fixture cliques - 2 merged away (9004/9005 vetoed)
    }
  }
}
