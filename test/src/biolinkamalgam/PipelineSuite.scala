package biolinkamalgam

import biolinkamalgam.export.DatasetFiles
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

      val files = DatasetFiles.files(out / "cliques")
      assert(files.nonEmpty)
      assert(!os.exists(out / "biolinkamalgam.parquet"))
      val back = DatasetFiles.cliques(spark, out)
      files.foreach { f =>
        val classes = spark.read.parquet(f.toString).select("CLASS").distinct().collect().map(_.getString(0))
        assert(classes.length == 1)
        assert(f.last == s"${biolinkamalgam.export.Splits.name(classes.head)}.parquet")
      }
      assert(back.count() == 13)
      assert(back.schema.fieldNames.toSeq == Seq("CURIE", "PREFERRED_NAME", "CLASS", "SYNONYMS", "TAXA"))
      assert(
        back.filter("CURIE = 'NCBIGene:2538'").head().getAs[scala.collection.Seq[String]]("TAXA").toList ==
          List("NCBITaxon:9606")
      )
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

    test("ClinVar + OBO: additive sources, per-predicate crossref files, core cache stays a hit") {
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
      // a previous run's leftovers in BOTH output dirs: run1 gains no edges, run2 has a stale predicate,
      // a pre-split single file and crash debris
      Seq(out / "run1", out / "run2").foreach { d =>
        os.write(d / "crossrefs" / "stale_predicate.parquet", "old", createFolders = true)
        os.write(d / "crossrefs" / "xref.parquet.parts" / "part-0.parquet", "debris", createFolders = true)
        os.write(d / "crossrefs.parquet", "old")
      }
      Pipeline.run(spark, opts(out / "run1").copy(clinvarFile = None, oboDir = None))
      val fp = os.read(out / "cache" / "_fingerprint")
      val stages = Pipeline.run(spark, opts(out / "run2"))
      // adding sources does not invalidate the multi-GB core cache
      assert(os.read(out / "cache" / "_fingerprint") == fp)
      // 4 ClinVar variations + 2 rs loci (rs268 shared by two alleles) + 3 active own-prefix OBO terms
      assert(stages.find(_.name == "ingest_extra").get.rows == 9)

      val back = DatasetFiles.cliques(spark, out / "run2")
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

      // one parquet per source; uppercase headers, no SOURCE column
      val xrefDir = out / "run2" / "crossrefs"
      val files = os.list(xrefDir).map(_.last).filterNot(_.endsWith(".crc")).toSet // stale + debris pruned
      assert(
        files == Set("clinvar.parquet", "obo_test.parquet")
      )
      assert(!os.exists(out / "run2" / "crossrefs.parquet"))
      def edges(source: String) = {
        val df = spark.read.parquet((xrefDir / s"$source.parquet").toString)
        assert(df.schema.fieldNames.toSeq == Seq("SUBJECT", "PREDICATE", "OBJECT"))
        df.collect().map(r => (r.getString(0), r.getString(1), r.getString(2))).toSet
      }
      val dbsnp = edges("clinvar")
      assert(dbsnp.contains(("CLINVAR:15043", "dbsnp_reference", "DBSNP:rs268")))
      assert(dbsnp.contains(("CLINVAR:15044", "dbsnp_reference", "DBSNP:rs268")))
      assert(edges("obo_test").contains(("TEST:0000002", "replaced_by", "TEST:0000003")))
      assert(edges("obo_test").contains(("TEST:0000001", "xref", "MESH:D000001")))
      assert(dbsnp.exists(_._2 == "gene_association"))
      val restored = DatasetFiles.crossrefs(spark, out / "run2")
      assert(restored.select("source").distinct().count() == 2)
      val card = os.read(out / "run2" / "README.md")
      assert(card.contains("config_name: crossrefs"))
      Seq("clinvar", "obo_test").foreach { s =>
        assert(card.contains(s"split: $s"))
        assert(card.contains(s"path: crossrefs/$s.parquet"))
      }
      assert(os.read(out / "run2" / "LICENSE.md").contains("TEST (OBO) | CC0-1.0"))
      // No extra sources: keep the second subset loadable with a schema-bearing zero-row split.
      assert(DatasetFiles.files(out / "run1" / "crossrefs").map(_.last) == Seq("empty.parquet"))
      assert(DatasetFiles.crossrefs(spark, out / "run1").count() == 0)
      assert(!os.exists(out / "run1" / "crossrefs.parquet"))
      assert(os.read(out / "run1" / "README.md").contains("config_name: crossrefs"))
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
      val first = DatasetFiles.cliques(spark, out).orderBy("CURIE").collect().toSeq
      assert(os.exists(cache / "_fingerprint"))

      os.remove.all(out / "cliques")
      Pipeline.run(spark, opts) // cache hit path
      val second = DatasetFiles.cliques(spark, out).orderBy("CURIE").collect().toSeq
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
      val altRows = DatasetFiles.cliques(spark, out / "alt-out").count()
      assert(altRows == 12) // the mouse-only NCBIGene:9003 clique now fails the allowlist
    }

    test("taxon inference: inferred taxa land in TAXA and out-of-cutoff species are dropped, cache untouched") {
      val out = os.temp.dir(prefix = "biolinkamalgam-taxa-test")
      val custom = out / "custom"
      os.makeDir.all(custom)
      os.copy(TestSupport.resource("/taxon-inference/inference.tsv"), custom / "inference.tsv")
      def opts(dest: os.Path, inference: Boolean) = Pipeline.Options(
        babelDir = Some(TestSupport.miniSnapshot),
        customDir = Some(custom),
        outDir = dest,
        taxaConfig = TestSupport.taxaConfig,
        promoteConfig = TestSupport.promoteConfig,
        filtersConfig = TestSupport.filtersConfig,
        ingestCache = Some(out / "cache"),
        curieTaxaConfig = if (inference) Some(TestSupport.resource("/config/curie_taxa.yaml")) else None
      )
      Pipeline.run(spark, opts(out / "plain", inference = false))
      val fp = os.read(out / "cache" / "_fingerprint")
      Pipeline.run(spark, opts(out / "inferred", inference = true))
      assert(os.read(out / "cache" / "_fingerprint") == fp) // inference sits after the cache

      def taxa(run: String) = DatasetFiles
        .cliques(spark, out / run)
        .collect()
        .map(r => r.getAs[String]("CURIE") -> r.getAs[scala.collection.Seq[String]]("TAXA").toList)
        .toMap
      val plain = taxa("plain")
      val inferred = taxa("inferred")
      // without inference the pig and rat cliques slip past the cutoff as taxonless rows
      assert(plain("ENSEMBL:ENSSSCG00000000002") == List.empty)
      assert(plain("RGD:2323195") == List.empty)
      // with inference: pig (9823) and rat (10116) are outside the test allowlist {9606, 10090} -> dropped
      assert(!inferred.contains("ENSEMBL:ENSSSCG00000000002"))
      assert(!inferred.contains("RGD:2323195"))
      assert(inferred("ENSEMBL:ENSG00000000003") == List("NCBITaxon:9606"))
      assert(inferred("MGI:7942167") == List("NCBITaxon:10090"))
      // unmappable identifiers stay taxonless and are kept, as the fullmap allowlist rule requires
      assert(inferred("ENSEMBL:2L52.1a.1") == List.empty)
      assert(inferred("UMLS:C0000001") == List.empty)
      // BABEL-tagged rows are unchanged
      assert(inferred("NCBIGene:2538") == List("NCBITaxon:9606"))
      assert(plain.size - inferred.size == 2)
      val allow = TestSupport.taxonAllowlist
      assert(inferred.values.forall(t => t.isEmpty || t.exists(allow.contains)))
      os.remove.all(out)
    }
  }
}
