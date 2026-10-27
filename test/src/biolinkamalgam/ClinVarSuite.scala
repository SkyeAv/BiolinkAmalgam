package biolinkamalgam

import biolinkamalgam.source.ClinVarSource
import org.apache.spark.sql.Row
import utest._

import scala.jdk.CollectionConverters._

/** Variants are not genes, and one rsID can reference several alleles. Each ClinVar allele stays distinct and rsIDs
  * become their own loci, never allele equivalence claims.
  */
object ClinVarSuite extends TestSuite {
  private val spark = TestSupport.spark

  private val cols = Seq("AlleleID", "Type", "Name", "GeneID", "RS# (dbSNP)", "Assembly", "VariationID")

  private def raw(rows: Seq[Seq[String]]) = {
    val schema = org.apache.spark.sql.types
      .StructType(cols.map(n => org.apache.spark.sql.types.StructField(n, org.apache.spark.sql.types.StringType)))
    spark.createDataFrame(rows.map(Row(_: _*)).asJava, schema)
  }

  private val sample = Seq(
    // Same VariationID on both assemblies: one variant.
    Seq("15041", "Indel", "NM_014855.3(AP5Z1):c.80_83delinsTGCT", "9907", "397704705", "GRCh37", "15041"),
    Seq("15041", "Indel", "NM_014855.3(AP5Z1):c.80_83delinsTGCT", "9907", "397704705", "GRCh38", "15041"),
    // rs sentinel -1 and missing gene: no crossrefs.
    Seq("15042", "Deletion", "NM_014855.3(AP5Z1):c.1413_1426del", "9907", "-1", "GRCh37", "15042"),
    // rsID shared by several alleles; two gene ids separated by ';'.
    Seq("15043", "Single nucleotide variant", "rs268", "9907", "268", "GRCh38", "15043")
  )

  val tests = Tests {
    test("assembly duplicates collapse; rs sentinel rows never crossref") {
      val variants = ClinVarSource.variants(raw(sample))
      assert(variants.count() == 3)
      val rows = variants
        .select("curie", "label", "rs")
        .collect()
        .map(r => r.getString(0) -> (r.getString(1), Option(r.getString(2))))
        .toMap
      assert(rows("CLINVAR:15041")._1.contains("AP5Z1"))
      assert(rows("CLINVAR:15042")._2.isEmpty)
      assert(rows("CLINVAR:15043")._2.contains("rs268"))
    }

    test("each allele and each rsID becomes its own sequence variant clique") {
      val cliques = new ClinVarSource(os.temp.dir() / "unused").transform(raw(sample))
      assert(cliques.count() == 5) // 3 ClinVar alleles + rs268 + rs397704705 DBSNP loci
      val byLeader = cliques
        .select("leader", "preferred_name", "names", "category")
        .collect()
        .map(r => r.getString(0) -> r)
        .toMap
      assert(
        byLeader.keySet == Set("CLINVAR:15041", "CLINVAR:15042", "CLINVAR:15043", "DBSNP:rs268", "DBSNP:rs397704705")
      )
      assert(byLeader("DBSNP:rs268").getSeq[String](2).toSet == Set("rs268", "DBSNP:rs268"))
      assert(byLeader.forall(_._2.getString(3) == "biolink:SequenceVariant"))
      val clinvarNames = byLeader("CLINVAR:15043").getSeq[String](2).toSet
      assert(clinvarNames.forall(!_.contains("397"))) // gene ids never become variant names
    }

    test("preferred-label equality cannot conflate alleles or rs loci") {
      val extra = Seq("15044", "Single nucleotide variant", "rs268", "9907", "268", "GRCh38", "15044")
      val source = new ClinVarSource(os.temp.dir() / "unused")
      val df = source
        .transform(raw(sample :+ extra))
        .withColumn("source", org.apache.spark.sql.functions.lit("clinvar"))
        .withColumn("priority", org.apache.spark.sql.functions.lit(10))
      val result = biolinkamalgam.transform.CliqueMerge(df, Set("NCBITaxon:9606"))
      assert(result.cliques.count() == 6)
      assert(result.merges.count() == 0)
    }

    test("crossrefs are typed edges; one rsID may have several alleles and genes") {
      val edges = ClinVarSource
        .crossrefs(raw(sample))
        .collect()
        .map(r => (r.getString(0), r.getString(1), r.getString(2)))
        .toSet
      assert(
        edges == Set(
          ("CLINVAR:15041", "dbsnp_reference", "DBSNP:rs397704705"),
          ("CLINVAR:15043", "dbsnp_reference", "DBSNP:rs268"),
          ("CLINVAR:15041", "gene_association", "NCBIGene:9907"),
          ("CLINVAR:15042", "gene_association", "NCBIGene:9907"),
          ("CLINVAR:15043", "gene_association", "NCBIGene:9907")
        )
      )
    }

    test("missing required columns and malformed rs values fail loudly") {
      assertThrows[IllegalArgumentException] {
        val dir = os.temp.dir()
        try {
          val f = dir / "bad.tsv"
          os.write(f, "AlleleID\tName\n1\tx\n")
          ClinVarSource.readSummary(spark, f)
        } finally os.remove.all(dir)
      }
      val malformed =
        sample :+ Seq("15044", "Single nucleotide variant", "-", "9907", "not-a-number", "GRCh38", "15044")
      val variants = ClinVarSource.variants(raw(malformed))
      assert(
        variants
          .filter(org.apache.spark.sql.functions.col("curie") === "CLINVAR:15044")
          .head()
          .getString(2) == null
      )
    }
  }
}
