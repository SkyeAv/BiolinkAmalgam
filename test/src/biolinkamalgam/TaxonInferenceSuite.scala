package biolinkamalgam

import biolinkamalgam.transform.{Filters, TaxonInference}
import org.apache.spark.sql.functions._
import utest._

/** Identifier-namespace taxon inference: taxonless cliques must not bypass the top-taxa allowlist when their
  * identifiers are single-species by construction (MGI, RGD, Ensembl species prefixes).
  */
object TaxonInferenceSuite extends TestSuite {
  private val spark = TestSupport.spark
  private val rules = TaxonInference.load(TestSupport.resource("/config/curie_taxa.yaml"))

  val tests = Tests {
    test("identifier lookup: CURIE prefixes, Ensembl species prefixes, bare ENS is human") {
      assert(TaxonInference.taxonOf("MGI:7942167", rules).contains("NCBITaxon:10090"))
      assert(TaxonInference.taxonOf("RGD:2323195", rules).contains("NCBITaxon:10116"))
      assert(TaxonInference.taxonOf("ENSEMBL:ENSG00000000003", rules).contains("NCBITaxon:9606"))
      assert(TaxonInference.taxonOf("ENSEMBL:ENSP00000000233.5", rules).contains("NCBITaxon:9606"))
      assert(TaxonInference.taxonOf("ENSEMBL:ENSSSCG00000000002", rules).contains("NCBITaxon:9823"))
      assert(TaxonInference.taxonOf("ENSEMBL:ENSMUSP00000000001", rules).contains("NCBITaxon:10090"))
      // non-stable-id ENSEMBL ids (WormBase-style transcripts) and unknown species prefixes never guess
      assert(TaxonInference.taxonOf("ENSEMBL:2L52.1a.1", rules).isEmpty)
      assert(TaxonInference.taxonOf("ENSEMBL:ENSZZZG00000000001", rules).isEmpty)
      assert(TaxonInference.taxonOf("ENSEMBL:ENSG0000000003", rules).isEmpty) // 10 digits
      // multi-species namespaces stay unmapped
      assert(TaxonInference.taxonOf("UMLS:C0000001", rules).isEmpty)
      assert(TaxonInference.taxonOf("nocolon", rules).isEmpty)
    }

    test("existing valid taxa always win; sentinel-only taxa are replaced; union of inferred taxa is sorted") {
      assert(TaxonInference.infer(Seq("NCBITaxon:7955"), Seq("MGI:1"), rules) == Seq("NCBITaxon:7955"))
      assert(TaxonInference.infer(Seq("NCBITaxon:0"), Seq("MGI:1"), rules) == Seq("NCBITaxon:10090"))
      assert(TaxonInference.infer(Seq.empty, Seq("UMLS:C1"), rules) == Seq.empty)
      assert(
        TaxonInference.infer(Seq.empty, Seq("RGD:1", "MGI:1", "ENSEMBL:ENSMUSG00000000001"), rules) ==
          Seq("NCBITaxon:10090", "NCBITaxon:10116")
      )
    }

    test("Spark expression matches the pure rule row for row") {
      import spark.implicits._
      val cases = Seq(
        (Seq("NCBITaxon:7955"), Seq("MGI:1")),
        (Seq("NCBITaxon:0"), Seq("MGI:1")),
        (Seq.empty[String], Seq("UMLS:C1")),
        (Seq.empty[String], Seq("RGD:1", "MGI:1", "ENSEMBL:ENSMUSG00000000001")),
        (Seq.empty[String], Seq("ENSEMBL:2L52.1a.1", "ENSEMBL:ENSSSCG00000000002")),
        (Seq.empty[String], Seq.empty[String]),
        (Seq.empty[String], Seq("MGI")) // colon-less identifier: no namespace, never infers
      )
      val df = cases.zipWithIndex.map { case ((t, i), n) => (n, t, i) }.toDF("n", "taxa", "identifiers")
      val got = TaxonInference(df, rules).orderBy("n").collect().map(_.getSeq[String](1).toSeq).toSeq
      assert(got == cases.map { case (t, i) => TaxonInference.infer(t, i, rules) })
      // null arrays survive as null (no inference possible, nothing to fill)
      val nulls =
        Seq((0, null.asInstanceOf[Seq[String]], null.asInstanceOf[Seq[String]])).toDF("n", "taxa", "identifiers")
      assert(TaxonInference(nulls, rules).head().isNullAt(1))
    }

    test("inferred taxa meet the allowlist: out-of-cutoff species drop, taxonless rows stay") {
      val tsv = TestSupport.resource("/taxon-inference/inference.tsv")
      val raw = new biolinkamalgam.source.CustomTableSource(tsv)
      val cliques = raw.transform(raw.read(spark, tsv))
      val allow = TestSupport.taxa.allowlist
      // Without inference all six are taxonless and every one bypasses the cutoff.
      assert(Filters.taxonAllowlisted(cliques, allow).count() == 6)
      val kept = Filters
        .taxonAllowlisted(TaxonInference(cliques, rules), allow)
        .select("leader", "taxa")
        .collect()
        .map(r => r.getString(0) -> r.getSeq[String](1).toSeq)
        .toMap
      assert(
        kept == Map(
          "ENSEMBL:ENSG00000000003" -> Seq("NCBITaxon:9606"),
          "MGI:7942167" -> Seq("NCBITaxon:10090"),
          "ENSEMBL:2L52.1a.1" -> Seq.empty,
          "UMLS:C0000001" -> Seq.empty
        )
      )
    }

    test("config rejects ENSEMBL as a whole-prefix mapping, malformed taxa and unknown keys") {
      val dir = os.temp.dir()
      try {
        def load(body: String) = { os.write.over(dir / "c.yaml", body); TaxonInference.load(dir / "c.yaml") }
        assertThrows[IllegalArgumentException](load("prefix_taxa:\n  ENSEMBL: NCBITaxon:9606\n"))
        assertThrows[IllegalArgumentException](load("prefix_taxa:\n  MGI: 10090\n"))
        assertThrows[IllegalArgumentException](load("ensembl_species_prefixes:\n  MUS: NCBITaxon:10090\n"))
        assertThrows[IllegalArgumentException](load("prefix_taxon:\n  MGI: NCBITaxon:10090\n"))
        assertThrows[IllegalArgumentException](TaxonInference.load(dir / "missing.yaml"))
        assert(load("prefix_taxa: {}\n").isEmpty)
      } finally os.remove.all(dir)
    }

    test("production config loads, maps every allowlisted model-organism namespace and keeps ENSGAC on BABEL taxon") {
      // The classpath copy must stay byte-identical to the real config, or tests pass on a drifted copy.
      val repoConfig = TestSupport.resource(
        "/config/curie_taxa.prod.yaml"
      ) / os.up / os.up / os.up / os.up / "config" / "curie_taxa.yaml"
      require(
        os.exists(repoConfig),
        s"cannot locate repo config/curie_taxa.yaml from ${TestSupport.resource("/config/curie_taxa.prod.yaml")}"
      )
      assert(os.read(repoConfig) == os.read(TestSupport.resource("/config/curie_taxa.prod.yaml")))
      val prod = TaxonInference.load(TestSupport.resource("/config/curie_taxa.prod.yaml"))
      assert(prod.ensemblSpeciesTaxa.size == 215)
      assert(prod.ensemblSpeciesTaxa("ENS") == "NCBITaxon:9606")
      assert(prod.ensemblSpeciesTaxa("ENSGAC") == "NCBITaxon:69293")
      assert(prod.prefixTaxa("MGI") == "NCBITaxon:10090")
      assert(!prod.prefixTaxa.keySet.exists(Set("UMLS", "MESH", "PR", "UniProtKB", "NCIT").contains))
    }
  }
}
