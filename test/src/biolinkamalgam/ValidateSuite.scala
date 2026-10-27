package biolinkamalgam

import biolinkamalgam.Main.Validate
import utest._

/** The pre-publication taxa checks that `validate` runs against a finished parquet. */
object ValidateSuite extends TestSuite {
  private val spark = TestSupport.spark
  private val allow = Set("NCBITaxon:9606", "NCBITaxon:10090")

  private def df(rows: (String, String, Seq[String])*) = {
    import spark.implicits._
    rows.toSeq.toDF("CURIE", "CLASS", "TAXA")
  }

  val tests = Tests {
    test("cutoff: allowlisted or taxonless rows pass; all-outside taxa fail") {
      val frame = df(
        ("A", "biolink:Gene", Seq("NCBITaxon:9606")),
        ("B", "biolink:Gene", Seq("NCBITaxon:7955", "NCBITaxon:9606")), // one allowlisted suffices
        ("C", "biolink:Gene", Seq.empty), // taxonless rows are legal
        ("D", "biolink:OrganismTaxon", Seq("NCBITaxon:224914")), // taxa themselves never filtered
        ("E", "biolink:Gene", Seq("NCBITaxon:9823")) // pig: all outside -> fails
      )
      val outside = Validate.taxonCutoffOutsideRows(frame, allow).collect().map(_.getString(0)).toSeq
      assert(outside == Seq("E"))
      assert(Validate.taxonCutoffOutside(frame, allow) == 1L)
    }

    test("empty allowlist disables the cutoff") {
      val frame = df(("E", "biolink:Gene", Seq("NCBITaxon:9823")))
      assert(Validate.taxonCutoffOutside(frame, Set.empty) == 0L)
    }

    test("shape: null or non-NCBITaxon CURIE elements anywhere in TAXA are violations") {
      import spark.implicits._
      val frame = df(
        ("A", "biolink:Gene", Seq("NCBITaxon:9606", "NCBITaxon:10090")),
        ("B", "biolink:Gene", Seq.empty)
      )
      assert(Validate.taxaShapeViolations(frame) == 0L)
      val bad = Seq(Seq("NCBITaxon:0"), Seq("9606"), Seq("NCBITaxon:9606", "taxon"))
        .toDF("TAXA") // sentinel; no prefix; one malformed element
      assert(Validate.taxaShapeViolations(bad) == 3L)
    }
  }
}
