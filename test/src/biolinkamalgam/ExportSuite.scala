package biolinkamalgam

import biolinkamalgam.export.{DatasetCard, Manifest, Parquet, Tsv}
import org.apache.spark.sql.Row
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
        lit("biolink:Gene").as("CLASS"),
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
      assert(back.schema.fieldNames.toSeq == Seq("CURIE", "PREFERRED_NAME", "CLASS", "SYNONYMS"))
      assert(back.select("CURIE").distinct().count() == 100)
      assert(back.head().getAs[scala.collection.Seq[String]]("SYNONYMS").toSeq == Seq("syn1", "syn2"))
    }

    test("binary concat replaces existing parquet only after writing the replacement") {
      val dir = os.temp.dir(prefix = "biolinkamalgam-export-replace")
      try {
        val dest = dir / "biolinkamalgam.parquet"
        Parquet.writeSingleFile(sampleDf(100), dest)
        Parquet.writeSingleFile(sampleDf(12), dest)
        val replacement = spark.read.parquet(dest.toString)
        assert(replacement.count() == 12)
        // count() may read only footer metadata; force column pages to detect stale Hadoop .crc files.
        assert(
          replacement.select("CURIE").collect().map(_.getString(0)).toSet == (0 until 12).map(i => s"CURIE:$i").toSet
        )
        Parquet.writeSingleFile(sampleDf(7), dest, simple = true)
        assert(spark.read.parquet(dest.toString).select("CURIE").collect().length == 7)
        assert(!os.exists(dir / "biolinkamalgam.parts"))
      } finally os.remove.all(dir)
    }

    test("single-file parquet: simple coalesce(1) fallback path") {
      val dir = os.temp.dir(prefix = "biolinkamalgam-export-simple")
      val dest = dir / "biolinkamalgam.parquet"
      Parquet.writeSingleFile(sampleDf(25), dest, simple = true)

      val back = spark.read.parquet(dest.toString)
      assert(back.count() == 25)
    }

    test("dataset card renders frontmatter, schema, and run stats that match the run") {
      val card = DatasetCard.render(
        "biolinkamalgam.parquet",
        Seq(Manifest.Stage("ingest", 15, 100), Manifest.Stage("arbitrate", 9, 50)),
        Map("babel" -> "test-snapshot"),
        "2026-01-01"
      )
      assert(card.startsWith("---\n"))
      assert(card.contains("size_categories:\n  - n<1K"))
      assert(card.contains("test-snapshot"))
      assert(card.contains("`SYNONYMS` | list<string>"))
      assert(card.contains("Generated 2026-01-01"))
      assert(!card.contains("null"))

      assert(DatasetCard.sizeCategory(0) == "n<1K")
      assert(DatasetCard.sizeCategory(999) == "n<1K")
      assert(DatasetCard.sizeCategory(1500) == "1K<n<10K")
      assert(DatasetCard.sizeCategory(5000000) == "1M<n<10M")
      assert(DatasetCard.sizeCategory(500000000) == "100M<n<1B")
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
