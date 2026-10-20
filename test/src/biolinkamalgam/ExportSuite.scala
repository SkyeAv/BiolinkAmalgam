package biolinkamalgam

import biolinkamalgam.export.{Parquet, Tsv}
import org.apache.spark.sql.functions._
import utest._

/** Export sinks: single-file parquet (fast binary concat + simple fallback) and TSV. */
object ExportSuite extends TestSuite {

  private val spark = TestSupport.spark

  private def sampleDf(rows: Int) =
    spark
      .range(0, rows)
      .select(
        concat(lit("CURIE:"), col("id")).as("CURIE"),
        lit("some name").as("PREFERRED_NAME"),
        lit("biolink:Gene").as("CATEGORY"),
        array(lit("syn1"), lit("syn2")).as("SYNONYMS")
      )
      .repartition(4)

  val tests = Tests {

    test("single-file parquet: fast binary-concat path re-reads losslessly") {
      val dir = os.temp.dir(prefix = "biolinkamalgam-export-fast")
      val dest = dir / "biolinkamalgam.parquet"
      Parquet.writeSingleFile(sampleDf(100), dest)

      assert(os.exists(dest))
      assert(!os.exists(dir / "biolinkamalgam.parts"))
      val back = spark.read.parquet(dest.toString)
      assert(back.count() == 100)
      assert(back.schema.fieldNames.toSeq == Seq("CURIE", "PREFERRED_NAME", "CATEGORY", "SYNONYMS"))
      assert(back.select("CURIE").distinct().count() == 100)
    }

    test("single-file parquet: simple coalesce(1) fallback path") {
      val dir = os.temp.dir(prefix = "biolinkamalgam-export-simple")
      val dest = dir / "biolinkamalgam.parquet"
      Parquet.writeSingleFile(sampleDf(25), dest, simple = true)

      val back = spark.read.parquet(dest.toString)
      assert(back.count() == 25)
    }

    test("TSV concat writes the header exactly once across part files") {
      import spark.implicits._
      val dir = os.temp.dir(prefix = "biolinkamalgam-export-tsv")
      val dest = dir / "conflicts.tsv"
      val df = Seq(("a", 1), ("b", 2), ("c", 3)).toDF("name", "n").repartition(3)

      Tsv.writeSingleFile(df, dest)

      val lines = os.read.lines(dest)
      assert(lines.head == "name\tn")
      assert(lines.count(_ == "name\tn") == 1)
      assert(lines.toSet == Set("name\tn", "a\t1", "b\t2", "c\t3"))
    }

    test("TSV export of an empty frame still writes the header") {
      import spark.implicits._
      val dir = os.temp.dir(prefix = "biolinkamalgam-export-tsv-empty")
      val dest = dir / "conflicts.tsv"
      val df = Seq.empty[(String, Int)].toDF("name", "n")

      Tsv.writeSingleFile(df, dest)

      assert(os.read.lines(dest) == Seq("name\tn"))
    }
  }
}
