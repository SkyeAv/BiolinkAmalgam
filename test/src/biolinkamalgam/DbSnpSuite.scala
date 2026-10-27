package biolinkamalgam

import biolinkamalgam.source.DbSnpSource
import org.apache.spark.sql.functions._
import utest._

object DbSnpSuite extends TestSuite {
  private val spark = TestSupport.spark
  private def fixture = Seq(
    """{"refsnp_id":"268","primary_snapshot_data":{"variant_type":"snv","allele_annotations":[{"assembly_annotation":[{"genes":[{"id":4023,"locus":"LPL"},{"id":4023,"locus":"LPL"}]}]}]},"citations":[12345,12345]}""",
    """{"refsnp_id":"94","withdrawn_snapshot_data":{"withdrawn_time":"2016-06-17T16:22Z"}}""",
    """{"refsnp_id":"120","merged_snapshot_data":{"merged_into":["268","269"]}}""",
    """{"refsnp_id":"269","unsupported_snapshot_data":{"alleles_from_previous_support":[]}}""",
    """{"refsnp_id":"270","nosnppos_snapshot_data":{"support":[]}}"""
  )
  private def withInput[A](lines: Seq[String])(f: os.Path => A): A = {
    val dir = os.temp.dir()
    try {
      val path = dir / "refsnp.json"
      os.write(path, lines.mkString("\n") + "\n")
      f(path)
    } finally os.remove.all(dir)
  }

  val tests = Tests {
    test("all lifecycle partitions project once, no allele or gene name becomes a locus alias") {
      withInput(fixture) { path =>
        val projected = DbSnpSource.project(DbSnpSource.read(spark, path))
        DbSnpSource.validate(projected)
        val statuses = projected.select("rsid", "status").collect().map(r => r.getString(0) -> r.getString(1)).toMap
        assert(
          statuses == Map(
            "268" -> "active",
            "94" -> "withdrawn",
            "120" -> "merged",
            "269" -> "unsupported",
            "270" -> "locationless"
          )
        )
        val output = DbSnpSource.output(projected)
        assert(output.count() == 3) // active + unsupported + locationless retained; history not active synonyms
        val locus = output.filter(col("CURIE") === "DBSNP:rs268").head()
        assert(locus.getAs[String]("PREFERRED_NAME") == "rs268")
        assert(locus.getSeq[String](locus.fieldIndex("SYNONYMS")).toSeq == Seq("rs268", "DBSNP:rs268"))
        assert(output.filter(col("CURIE") === "DBSNP:rs120").count() == 0)
        val edges = DbSnpSource
          .crossrefs(projected)
          .collect()
          .map(r => (r.getString(0), r.getString(1), r.getString(2)))
          .toSet
        assert(
          edges == Set(
            ("DBSNP:rs268", "gene_association", "NCBIGene:4023"),
            ("DBSNP:rs268", "cites", "PMID:12345"),
            ("DBSNP:rs120", "merged_into", "DBSNP:rs268"),
            ("DBSNP:rs120", "merged_into", "DBSNP:rs269")
          )
        )
      }
    }

    test("corrupt JSON, invalid ids, missing/contradictory status and duplicate rsIDs fail") {
      val invalid = Seq(
        """{"refsnp_id":"0","primary_snapshot_data":{}}""",
        """{"refsnp_id":"268"}""",
        """{"refsnp_id":"268","primary_snapshot_data":{},"withdrawn_snapshot_data":{}}""",
        """{"refsnp_id":"120","merged_snapshot_data":{"merged_into":[]}}"""
      )
      invalid.foreach { line =>
        withInput(Seq(line)) { path =>
          assertThrows[IllegalArgumentException](
            DbSnpSource.validate(DbSnpSource.project(DbSnpSource.read(spark, path)))
          )
        }
      }
      withInput(Seq(fixture.head, fixture.head)) { path =>
        assertThrows[IllegalArgumentException](DbSnpSource.validate(DbSnpSource.project(DbSnpSource.read(spark, path))))
      }
      withInput(Seq("{broken")) { path =>
        assertThrows[org.apache.spark.SparkException](DbSnpSource.read(spark, path).collect())
      }
    }

    test("missing optional assembly annotations do not erase present gene associations") {
      val line = """{"refsnp_id":"1","primary_snapshot_data":{"allele_annotations":[{},
        {"assembly_annotation":[{}, {"genes":[{"id":4023}]}]}]}}""".replace("\n", "")
      withInput(Seq(line)) { path =>
        val projected = DbSnpSource.project(DbSnpSource.read(spark, path))
        DbSnpSource.validate(projected)
        assert(projected.select("genes").head().getSeq[String](0).toSeq == Seq("4023"))
      }
    }

    test("official b157 sample and first exceptional records match partial schema") {
      val projected = DbSnpSource.project(DbSnpSource.read(spark, TestSupport.resource("/dbsnp/mini.jsonl")))
      DbSnpSource.validate(projected)
      assert(projected.count() == 6)
      val rs268 = projected.filter(col("rsid") === "268").head()
      assert(rs268.getSeq[String](rs268.fieldIndex("genes")).toSeq.contains("4023"))
      assert(
        projected.select("status").collect().map(_.getString(0)).toSet ==
          Set("active", "merged", "withdrawn", "unsupported", "locationless")
      )
    }

    test("explicit schema supports splittable bzip2 input without staging decompressed JSON") {
      val dir = os.temp.dir()
      try {
        val path = dir / "refsnp.json.bz2"
        val stream = new org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream(
          java.nio.file.Files.newOutputStream(path.toNIO)
        )
        try stream.write((fixture.mkString("\n") + "\n").getBytes(java.nio.charset.StandardCharsets.UTF_8))
        finally stream.close()
        val projected = DbSnpSource.project(DbSnpSource.read(spark, path))
        DbSnpSource.validate(projected)
        assert(projected.count() == 5)
        assert(DbSnpSource.read(spark, path).schema == DbSnpSource.schema)
        val codec = new org.apache.hadoop.io.compress.CompressionCodecFactory(spark.sparkContext.hadoopConfiguration)
          .getCodec(new org.apache.hadoop.fs.Path(path.toString))
        assert(codec.isInstanceOf[org.apache.hadoop.io.compress.SplittableCompressionCodec])
      } finally os.remove.all(dir)
    }
  }
}
