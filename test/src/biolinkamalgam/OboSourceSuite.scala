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
        assertThrows[org.apache.spark.SparkException](
          new OboSource(bad, "TEST", "biolink:NamedThing").read(spark, bad).count()
        )
        val headerOnly = dir / "header.obo"
        os.write(headerOnly, "format-version: 1.4\n")
        assertThrows[org.apache.spark.SparkException](
          new OboSource(headerOnly, "TEST", "biolink:NamedThing").read(spark, headerOnly).count()
        )
        assertThrows[IllegalArgumentException](new OboSource(obo, "1bad", "biolink:NamedThing"))
      } finally os.remove.all(dir)
    }
    test("quoted punctuation, escaped backslashes and gzip preserve original strings") {
      val text = """[Term]
                   |id: TEST:99
                   |name: punctuation
                   |synonym: "keep ! {braces} \"quote\"" EXACT [X:1 "citation"] ! trailing comment
                   |synonym: "literal \\n text" EXACT []
                   |""".stripMargin
      val term = OboTerms.parseTerms(text.linesIterator, _ => true).next()
      assert(term.exact.head == "keep ! {braces} \"quote\"")
      assert(term.exact(1) == "literal \\n text")
      val dir = os.temp.dir()
      try {
        val gz = dir / "test.obo.gz"
        val out = new java.util.zip.GZIPOutputStream(java.nio.file.Files.newOutputStream(gz.toNIO))
        try out.write(os.read.bytes(obo))
        finally out.close()
        val source = new OboSource(gz, "TEST", "biolink:PhenotypicFeature", Seq("NCBITaxon:9606"))
        val rows = source.transform(source.read(spark, gz))
        assert(rows.count() == 3)
        assert(rows.select("taxa").head().getSeq[String](0) == Seq("NCBITaxon:9606"))
      } finally os.remove.all(dir)
    }

    test("registry binds one reviewed source per ontology file and fails on gaps") {
      val dir = os.temp.dir()
      try {
        os.copy(obo, dir / "test.obo")
        val cfg = dir / "obo_sources.yaml"
        os.write(
          cfg,
          """ontologies:
                          |  - prefix: TEST
                          |    url: http://example.org/test.obo
                          |    category: biolink:PhenotypicFeature
                          |    license: CC-BY-4.0
                          |    taxa: []
                          |""".stripMargin
        )
        val entries = biolinkamalgam.source.OboRegistry.entries(cfg)
        assert(entries.map(_.prefix) == Seq("TEST"))
        val sources = biolinkamalgam.source.OboRegistry.sources(dir, entries)
        assert(sources.map(_.name) == Seq("obo:TEST"))
        os.copy(obo, dir / "test.obo.gz")
        assertThrows[IllegalArgumentException](biolinkamalgam.source.OboRegistry.sources(dir, entries))
        os.remove(dir / "test.obo.gz")
        os.write(
          dir / "duplicate.yaml",
          os.read(cfg) + "  - prefix: test\n    url: http://example.org/2.obo\n" +
            "    category: biolink:PhenotypicFeature\n    license: CC0-1.0\n    taxa: []\n"
        )
        assertThrows[IllegalArgumentException](biolinkamalgam.source.OboRegistry.entries(dir / "duplicate.yaml"))
        os.remove(dir / "test.obo")
        assertThrows[IllegalArgumentException](biolinkamalgam.source.OboRegistry.sources(dir, entries))
        os.write(dir / "bad.yaml", "ontologies:\n  - prefix: X\n    bogus: 1\n")
        assertThrows[IllegalArgumentException](biolinkamalgam.source.OboRegistry.entries(dir / "bad.yaml"))
      } finally os.remove.all(dir)
    }
  }
}
