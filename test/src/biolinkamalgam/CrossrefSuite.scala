package biolinkamalgam

import biolinkamalgam.transform.CrossrefAnalysis
import utest._

object CrossrefSuite extends TestSuite {
  private val spark = TestSupport.spark
  import spark.implicits._

  val tests = Tests {
    test("typed associations deduplicate and degrees do not conflate predicates or sources") {
      val input = Seq(
        ("CLINVAR:1", "dbsnp_reference", "DBSNP:rs1", "clinvar"),
        ("CLINVAR:2", "dbsnp_reference", "DBSNP:rs1", "clinvar"),
        ("CLINVAR:2", "dbsnp_reference", "DBSNP:rs1", "clinvar"),
        ("CLINVAR:2", "dbsnp_reference", "DBSNP:rs1", "another_source"),
        ("CLINVAR:1", "gene_association", "NCBIGene:1", "clinvar"),
        ("CLINVAR:2", "gene_association", "NCBIGene:1", "clinvar"),
        ("X:1", "xref", "DBSNP:rs1", "obo:X"),
        (null, "xref", "X:1", "obo:X")
      ).toDF("subject", "predicate", "object", "source")
      val clean = CrossrefAnalysis.edges(input)
      assert(clean.count() == 6)
      val summary = CrossrefAnalysis.summary(clean)
      assert(
        summary.filter("predicate = 'dbsnp_reference' AND source = 'clinvar'").select("edges").head().getLong(0) == 2
      )
      val degree = CrossrefAnalysis.highDegree(clean, 1)
      val rs = degree.filter("predicate = 'dbsnp_reference' AND direction = 'in'").head()
      assert(rs.getAs[String]("node") == "DBSNP:rs1")
      assert(rs.getAs[Long]("neighbors") == 2) // another source is not another allele
      assert(degree.filter("predicate = 'xref'").count() == 0) // no predicate mixing
    }

    test("empty graph and invalid bounds") {
      val empty = Seq.empty[(String, String, String, String)].toDF("subject", "predicate", "object", "source")
      assert(CrossrefAnalysis.summary(empty).count() == 0)
      assert(CrossrefAnalysis.highDegree(empty, 1).count() == 0)
      assertThrows[IllegalArgumentException](CrossrefAnalysis.highDegree(empty, 0))
      assertThrows[IllegalArgumentException](CrossrefAnalysis.edges(empty.drop("source")))
    }
  }
}
