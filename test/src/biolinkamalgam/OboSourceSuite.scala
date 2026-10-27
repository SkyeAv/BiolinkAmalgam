package biolinkamalgam

import biolinkamalgam.source.{OboSource, OboTerms}
import utest._

/** EXACT synonyms and alt_ids are equivalence; BROAD/NARROW/RELATED are not. Obsolete terms and foreign-prefix terms
  * never enter the dataset.
  */
object OboSourceSuite extends TestSuite {
  private val spark = TestSupport.spark
  private val obo = TestSupport.resource("/obo/mini.obo")

  val tests = Tests {
    test("parser takes EXACT synonyms, alt_ids and names; skips obsolete, broad and related") {
      val terms = OboTerms.parseTerms(os.read.lines(obo).iterator, _ => true).toSeq
      assert(terms.map(_.id) == Seq("TEST:0000001", "TEST:0000002", "FOREIGN:0000003", "TEST:0000004", "TEST:0000005"))
      val t1 = terms.head
      assert(t1.exact == Seq("test process exact variant"))
      assert(t1.altIds == Seq("TEST:9000001"))
      assert(!t1.obsolete)
      val obsolete = terms(1)
      assert(obsolete.obsolete && obsolete.replacedBy == Seq("TEST:0000003"))
      assert(terms(3).name.isEmpty)
    }

    test("source keeps own-prefix active terms and drops foreign imports") {
      val cliques = new OboSource(obo, "TEST", "biolink:PhenotypicFeature").transform(
        new OboSource(obo, "TEST", "biolink:PhenotypicFeature").read(spark, obo)
      )
      val byLeader = cliques
        .select("leader", "preferred_name", "names", "category")
        .collect()
        .map(r => r.getString(0) -> r)
        .toMap
      assert(byLeader.keySet == Set("TEST:0000001", "TEST:0000004", "TEST:0000005"))
      val names = byLeader("TEST:0000001").getSeq[String](2).toSet
      assert(names == Set("test process", "test process exact variant", "TEST:9000001"))
      assert(!names.exists(_.contains("broad")))
      assert(!names.exists(_.contains("related")))
      assert(byLeader("TEST:0000005").getString(1) == "escaped characters  colon:")
      assert(byLeader.forall(_._2.getString(3) == "biolink:PhenotypicFeature"))
      assert(cliques.filter(org.apache.spark.sql.functions.col("preferred_name").isNull).count() == 0)
    }

    test("non-OBO files and invalid prefixes fail loudly") {
      val dir = os.temp.dir()
      try {
        val bad = dir / "bad.obo"
        os.write(bad, "hello world\n")
        assertThrows[IllegalArgumentException](new OboSource(bad, "TEST", "biolink:NamedThing").read(spark, bad))
        val headerOnly = dir / "header.obo"
        os.write(headerOnly, "format-version: 1.4\n")
        assertThrows[IllegalArgumentException](
          new OboSource(headerOnly, "TEST", "biolink:NamedThing").read(spark, headerOnly)
        )
        assertThrows[IllegalArgumentException](new OboSource(obo, "1bad", "biolink:NamedThing"))
      } finally os.remove.all(dir)
    }
  }
}
