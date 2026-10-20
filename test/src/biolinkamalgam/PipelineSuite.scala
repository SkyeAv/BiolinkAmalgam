package biolinkamalgam

import utest._

/** End-to-end local run: mini fixture snapshot + custom TSV → parquet + reports. */
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
          promoteConfig = TestSupport.promoteConfig
        )
      )

      assert(stages.map(_.name) == Seq("ingest", "merge", "arbitrate", "export"))
      assert(stages.find(_.name == "ingest").get.rows == 14)
      assert(stages.find(_.name == "merge").get.rows == 11)
      assert(stages.find(_.name == "arbitrate").get.rows == 11)

      // the deliverable: one parquet file
      val parquet = out / "biolinkamalgam.parquet"
      assert(os.exists(parquet))
      val back = spark.read.parquet(parquet.toString)
      assert(back.count() == 11)
      assert(back.schema.fieldNames.toSeq == Seq("CURIE", "PREFERRED_NAME", "CATEGORY", "SYNONYMS"))

      // audit reports
      val conflicts = os.read.lines(out / "conflicts.tsv")
      assert(conflicts.length == 3) // header + glucose + shared alias
      val merges = os.read.lines(out / "merges.tsv")
      assert(merges.length == 4) // header + 3 merges
    }
  }
}
