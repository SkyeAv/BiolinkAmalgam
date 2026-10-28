package biolinkamalgam

import biolinkamalgam.export.{DatasetCard, Parquet, Splits, Tsv}
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
        array(lit("syn1"), lit("syn2")).as("SYNONYMS"),
        array(lit("NCBITaxon:9606")).as("TAXA")
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
      assert(back.schema.fieldNames.toSeq == Seq("CURIE", "PREFERRED_NAME", "CLASS", "SYNONYMS", "TAXA"))
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

    test("split names are hub-safe, deterministic and collision checked") {
      assert(Splits.name("biolink:Gene") == "gene")
      assert(Splits.name("biolink:SmallMolecule") == "small_molecule")
      assert(Splits.name("biolink:RNAProduct") == "rna_product")
      assert(Splits.name("obo:HsapDv") == "obo_hsapdv")
      assertThrows[IllegalArgumentException](Splits.name(null))
      assertThrows[IllegalArgumentException](Splits.name(""))
      assertThrows[IllegalArgumentException](Splits.name("..."))
      assertThrows[IllegalArgumentException](Splits.mapping(Seq("obo:MP", "obo_MP")))
    }

    test("partitioned parquet writes each split once, drops partition column, sorts and prunes stale files") {
      val dir = os.temp.dir(prefix = "biolinkamalgam-partitioned") / "crossrefs"
      os.write(dir / "stale.parquet", "old", createFolders = true)
      val input = spark
        .range(0, 31)
        .select(
          concat(lit("CURIE:"), col("id")).as("SUBJECT"),
          lit("xref").as("PREDICATE"),
          lit("TARGET:1").as("OBJECT"),
          when(col("id") % 2 === 0, lit("clinvar")).otherwise(lit("obo_mp")).as("__split")
        )
        .repartition(4)
      val splits = Parquet.writePartitioned(input, "__split", dir, sortBy = Seq("SUBJECT", "PREDICATE", "OBJECT"))
      assert(splits == Seq("clinvar", "obo_mp"))
      assert(!os.exists(dir / "stale.parquet"))
      assert(!os.exists(dir / os.up / "crossrefs.parts"))
      splits.foreach { s =>
        val f = dir / s"$s.parquet"
        val df = spark.read.parquet(f.toString)
        assert(df.columns.toSeq == Seq("SUBJECT", "PREDICATE", "OBJECT"))
        val subjects = df.collect().map(_.getString(0)).toSeq
        assert(subjects == subjects.sorted)
        assert(Parquet.rowCount(f) == (if (s == "clinvar") 16L else 15L))
      }
      assert(Parquet.writePartitioned(input.limit(0), "__split", dir).isEmpty)
      assert(os.list(dir).isEmpty)
    }

    test("dataset card renders two subsets, class/source splits and upstream terms") {
      val card = DatasetCard.render(
        Seq(DatasetCard.Split("gene", "biolink:Gene", 9)),
        Seq.empty,
        Seq.empty,
        Map("babel" -> "test-snapshot"),
        "2026-01-01"
      )
      assert(card.startsWith("---\n"))
      assert(card.contains("size_categories:\n  - n<1K"))
      assert(card.contains("test-snapshot"))
      assert(card.contains("`SYNONYMS` | list<string>"))
      assert(card.contains("`TAXA` | list<string>"))
      assert(card.contains("Generated 2026-01-01"))
      assert(!card.contains("null"))

      assert(!card.contains("crossrefs_")) // no edges => only the default config

      val withEdges = DatasetCard.render(
        Seq(
          DatasetCard.Split("small_molecule", "biolink:SmallMolecule", 3),
          DatasetCard.Split("gene", "biolink:Gene", 6)
        ),
        Seq(DatasetCard.Split("obo_mp", "obo:MP", 2), DatasetCard.Split("clinvar", "clinvar", 4)),
        Seq("xref", "dbsnp_reference"),
        Map("babel" -> "test-snapshot", "clinvar" -> "vs.txt"),
        "2026-01-01"
      )
      val frontmatter = withEdges.split("\n---\n", 2).head
      assert(frontmatter.sliding("config_name:".length).count(_ == "config_name:") == 2)
      assert(frontmatter.contains("config_name: cliques\n    default: true"))
      assert(frontmatter.contains("config_name: crossrefs\n    data_files:"))
      assert(frontmatter.contains("split: gene\n        path: cliques/gene.parquet"))
      assert(frontmatter.contains("split: clinvar\n        path: crossrefs/clinvar.parquet"))
      assert(!frontmatter.contains("split: train"))
      assert(frontmatter.indexOf("split: gene") < frontmatter.indexOf("split: small_molecule"))
      assert(withEdges.contains("load_dataset(\"SkyeAv/biolink-amalgam\", \"crossrefs\", split=\"clinvar\")"))
      assert(withEdges.contains("\nedges = load_dataset("))
      val emptyEdges = DatasetCard.render(
        Seq(DatasetCard.Split("gene", "biolink:Gene", 9)),
        Seq(DatasetCard.Split("empty", "none", 0)),
        Seq.empty,
        Map.empty,
        "2026-01-01"
      )
      assert(emptyEdges.contains("split=\"empty\", streaming=True)"))
      assert(withEdges.contains("`SUBJECT` | string"))
      assert(!withEdges.contains("`source`"))
      assert(frontmatter.contains("license_link: LICENSE.md"))
      assertThrows[IllegalArgumentException](DatasetCard.Split("../x", "x", 0))
      assertThrows[IllegalArgumentException](
        DatasetCard.render(Seq.empty, Seq.empty, Seq("../x"), Map.empty, "2026-01-01")
      )
      val license = DatasetCard.license(Map("gtdb" -> "metadata", "clinvar" -> "variants"), Map("MP" -> "CC-BY-4.0"))
      assert(license.contains("UMLS Metathesaurus License"))
      assert(license.contains("CC-BY-SA-4.0"))
      assert(license.contains("MP (OBO) | CC-BY-4.0"))
      assert(license.contains("Neither code"))

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
