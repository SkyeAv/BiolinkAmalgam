package biolinkamalgam

import utest._

/** End-to-end local run: mini fixture snapshot + custom TSV -> parquet + reports, with the declarative ingest filters
  * (category drops + taxon allowlist) active.
  */
object PipelineSuite extends TestSuite {

  private val spark = TestSupport.spark

  val tests = Tests {

    test("full pipeline on fixtures produces the canonical deliverables") {
      val out = os.temp.dir(prefix = "biolinkamalgam-pipeline-test")
      val stages = Pipeline.run(
        spark,
        Pipeline.Options(
          babelDir = Some(TestSupport.miniSnapshot),
          customDir = Some(TestSupport.customDir),
          outDir = out,
          taxaConfig = TestSupport.taxaConfig,
          promoteConfig = TestSupport.promoteConfig,
          filtersConfig = TestSupport.filtersConfig
        )
      )

      // 19 fixture cliques - biolink:Publication - biolink:Agent (category drops)
      // - NCBITaxon:1111111 and NCBITaxon:2222222 cliques (taxon allowlist)
      assert(stages.map(_.name) == Seq("ingest", "merge", "arbitrate", "export"))
      assert(stages.find(_.name == "ingest").get.rows == 15)
      // merges: HGNC:9002 and CUSTOM:2 fold into NCBIGene:9001 (the 9004/9005
      // merge is gone — both cliques failed the allowlist)
      assert(stages.find(_.name == "merge").get.rows == 13)
      assert(stages.find(_.name == "arbitrate").get.rows == 13)

      // the deliverable: one parquet file
      val parquet = out / "biolinkamalgam.parquet"
      assert(os.exists(parquet))
      val back = spark.read.parquet(parquet.toString)
      assert(back.count() == 13)
      assert(back.schema.fieldNames.toSeq == Seq("CURIE", "PREFERRED_NAME", "CLASS", "SYNONYMS"))
      // dropped categories never reach the deliverable
      assert(back.filter("CLASS = 'biolink:Publication'").count() == 0)
      assert(back.filter("CLASS = 'biolink:Agent'").count() == 0)
      assert(back.filter("CURIE = 'PMID:99999999'").count() == 0)
      assert(back.filter("CURIE = 'UMLS:C8888888'").count() == 0)

      // HF dataset card always; audit TSVs only with --audit
      assert(os.exists(out / "README.md"))
      assert(os.read(out / "README.md").contains("size_categories:"))
      assert(!os.exists(out / "conflicts.tsv"))
      assert(!os.exists(out / "merges.tsv"))

      Pipeline.run(
        spark,
        Pipeline.Options(
          babelDir = Some(TestSupport.miniSnapshot),
          customDir = Some(TestSupport.customDir),
          outDir = out / "audit",
          taxaConfig = TestSupport.taxaConfig,
          promoteConfig = TestSupport.promoteConfig,
          filtersConfig = TestSupport.filtersConfig,
          audit = true
        )
      )
      val conflicts = os.read.lines(out / "audit" / "conflicts.tsv")
      assert(conflicts.length == 3) // header + glucose + shared alias
      val merges = os.read.lines(out / "audit" / "merges.tsv")
      assert(merges.length == 3) // header + 2 merges
    }

    test("ClinVar + OBO: additive sources, typed crossref sidecar, core cache stays a hit") {
      val out = os.temp.dir(prefix = "biolinkamalgam-extra-test")
      val oboDir = out / "obo"
      os.makeDir.all(oboDir)
      os.copy(TestSupport.resource("/obo/mini.obo"), oboDir / "test.obo")
      val oboCfg = out / "obo_sources.yaml"
      os.write(
        oboCfg,
        "ontologies:\n  - prefix: TEST\n    url: http://example.org/test.obo\n    category: biolink:PhenotypicFeature\n" +
          "    license: CC0-1.0\n    taxa: []\n"
      )
      def opts(dest: os.Path) = Pipeline.Options(
        babelDir = Some(TestSupport.miniSnapshot),
        customDir = Some(TestSupport.customDir),
        outDir = dest,
        taxaConfig = TestSupport.taxaConfig,
        promoteConfig = TestSupport.promoteConfig,
        filtersConfig = TestSupport.filtersConfig,
        ingestCache = Some(out / "cache"),
        clinvarFile = Some(TestSupport.resource("/clinvar/variant_summary.txt")),
        oboDir = Some(oboDir),
        oboConfig = Some(oboCfg)
      )
      Pipeline.run(spark, opts(out / "run1").copy(clinvarFile = None, oboDir = None))
      val fp = os.read(out / "cache" / "_fingerprint")
      val stages = Pipeline.run(spark, opts(out / "run2"))
      // adding sources does not invalidate the multi-GB core cache
      assert(os.read(out / "cache" / "_fingerprint") == fp)
      // 4 ClinVar variations + 2 rs loci (rs268 shared by two alleles) + 3 active own-prefix OBO terms
      assert(stages.find(_.name == "ingest_extra").get.rows == 9)

      val back = spark.read.parquet((out / "run2" / "biolinkamalgam.parquet").toString)
      val variants =
        back.filter("CLASS = 'biolink:SequenceVariant'").select("CURIE").collect().map(_.getString(0)).toSet
      assert(
        variants == Set(
          "CLINVAR:15041",
          "CLINVAR:15042",
          "CLINVAR:15043",
          "CLINVAR:15044",
          "DBSNP:rs268",
          "DBSNP:rs397704705"
        )
      )
      assert(back.filter("CURIE = 'TEST:0000001'").count() == 1)
      assert(back.filter("CURIE = 'TEST:0000002'").count() == 0) // obsolete

      val edges = spark.read.parquet((out / "run2" / "crossrefs.parquet").toString)
      assert(edges.schema.fieldNames.toSeq == Seq("subject", "predicate", "object", "source"))
      val rows = edges.collect().map(r => (r.getString(0), r.getString(1), r.getString(2))).toSet
      assert(rows.contains(("CLINVAR:15043", "dbsnp_reference", "DBSNP:rs268")))
      assert(rows.contains(("CLINVAR:15044", "dbsnp_reference", "DBSNP:rs268")))
      assert(rows.contains(("TEST:0000002", "replaced_by", "TEST:0000003")))
      assert(rows.contains(("TEST:0000001", "xref", "MESH:D000001")))
      assert(os.read(out / "run2" / "README.md").contains("crossrefs.parquet"))
      // no extra sources => no sidecar
      assert(!os.exists(out / "run1" / "crossrefs.parquet"))
    }

    test("ingest cache: second run reuses materialized cliques with identical output") {
      val out = os.temp.dir(prefix = "biolinkamalgam-cache-test")
      val cache = out / "ingest-cache"
      def opts = Pipeline.Options(
        babelDir = Some(TestSupport.miniSnapshot),
        customDir = Some(TestSupport.customDir),
        outDir = out,
        taxaConfig = TestSupport.taxaConfig,
        promoteConfig = TestSupport.promoteConfig,
        filtersConfig = TestSupport.filtersConfig,
        ingestCache = Some(cache)
      )
      Pipeline.run(spark, opts)
      val first = spark.read.parquet((out / "biolinkamalgam.parquet").toString).orderBy("CURIE").collect().toSeq
      assert(os.exists(cache / "_fingerprint"))

      os.remove(out / "biolinkamalgam.parquet")
      Pipeline.run(spark, opts) // cache hit path
      val second = spark.read.parquet((out / "biolinkamalgam.parquet").toString).orderBy("CURIE").collect().toSeq
      assert(first.map(_.toString) == second.map(_.toString))

      // changing an ingest-affecting config (taxon allowlist) must invalidate
      val staging = out / "alt-config"
      os.makeDir.all(staging)
      os.write.over(
        staging / "taxa.yaml",
        os.read(TestSupport.taxaConfig).replace("  - NCBITaxon:10090\n", "")
      )
      Pipeline.run(
        spark,
        opts.copy(taxaConfig = staging / "taxa.yaml", outDir = out / "alt-out")
      )
      val altRows = spark.read.parquet((out / "alt-out" / "biolinkamalgam.parquet").toString).count()
      assert(altRows == 12) // the mouse-only NCBIGene:9003 clique now fails the allowlist
    }
  }
}
