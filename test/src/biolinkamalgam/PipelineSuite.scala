package biolinkamalgam

import utest._

/** End-to-end local run: mini fixture snapshot + custom TSV -> parquet + reports, with the declarative ingest
  * filters (category drops + taxon allowlist) active.
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

      // 15 fixture cliques - biolink:Publication - biolink:Agent (category drops)
      // - NCBITaxon:1111111 and NCBITaxon:2222222 cliques (taxon allowlist)
      assert(stages.map(_.name) == Seq("ingest", "merge", "arbitrate", "export"))
      assert(stages.find(_.name == "ingest").get.rows == 11)
      // merges: HGNC:9002 and CUSTOM:2 fold into NCBIGene:9001 (the 9004/9005
      // merge is gone — both cliques failed the allowlist)
      assert(stages.find(_.name == "merge").get.rows == 9)
      assert(stages.find(_.name == "arbitrate").get.rows == 9)

      // the deliverable: one parquet file
      val parquet = out / "biolinkamalgam.parquet"
      assert(os.exists(parquet))
      val back = spark.read.parquet(parquet.toString)
      assert(back.count() == 9)
      assert(back.schema.fieldNames.toSeq == Seq("CURIE", "PREFERRED_NAME", "CLASS", "SYNONYMS"))
      // dropped categories never reach the deliverable
      assert(back.filter("CLASS = 'biolink:Publication'").count() == 0)
      assert(back.filter("CLASS = 'biolink:Agent'").count() == 0)
      assert(back.filter("CURIE = 'PMID:99999999'").count() == 0)
      assert(back.filter("CURIE = 'UMLS:C8888888'").count() == 0)

      // audit reports + the HF dataset card
      assert(os.exists(out / "README.md"))
      assert(os.read(out / "README.md").contains("size_categories:"))
      val conflicts = os.read.lines(out / "conflicts.tsv")
      assert(conflicts.length == 3) // header + glucose + shared alias
      val merges = os.read.lines(out / "merges.tsv")
      assert(merges.length == 3) // header + 2 merges
    }
  }
}
