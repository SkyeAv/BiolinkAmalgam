package biolinkamalgam

import biolinkamalgam.config.Config.FilterConfig
import biolinkamalgam.transform.{Filters, TermGates}
import utest._

/** Term admission gates and ingest filters — the user-facing exclusion rules. Pure tests, no Spark. */
object FilterSuite extends TestSuite {

  private val cfg: FilterConfig = TestSupport.filters

  val tests = Tests {

    test("token_qc parity: empty, control chars, uncharacterized, hypothetical") {
      assert(!TermGates.tokenQc("", cfg.bannedSubstrings))
      assert(!TermGates.tokenQc("line\nbreak", cfg.bannedSubstrings))
      assert(!TermGates.tokenQc("tab\tvalue", cfg.bannedSubstrings))
      assert(!TermGates.tokenQc("uncharacterized protein c1orf2", cfg.bannedSubstrings))
      assert(!TermGates.tokenQc("hypothetical protein xp123", cfg.bannedSubstrings))
      assert(TermGates.tokenQc("brca1", cfg.bannedSubstrings))
    }

    test("language substring ban") {
      assert(!TermGates.tokenQc("english language", cfg.bannedSubstrings))
      assert(!TermGates.tokenQc("python programming language", cfg.bannedSubstrings))
      // whole-word entities that merely contain the letters are still banned —
      // substring semantics, matching token_qc's literal-needle check
      assert(TermGates.tokenQc("glucose", cfg.bannedSubstrings))
    }

    test("numeric junk: decimals, signs, exponents — not just integers") {
      Seq("0.5", "1.05", "-3", "+42", "1e5", "2.5e-3", ".5", "51").foreach { n =>
        assert(TermGates.isDeadTerm(n, cfg.deadTerms, cfg.compiledExcludeNameRegex))
      }
      // identifiers and gene-like tokens survive
      Seq("bcl2", "g6pc1", "tnf-alpha", "3-methylcholanthrene").foreach { n =>
        assert(!TermGates.isDeadTerm(n, cfg.deadTerms, cfg.compiledExcludeNameRegex))
      }
    }

    test("dead terms: fullmap sentinel baseline plus stopwords") {
      // fullmap is_dead_term baseline
      Seq(
        "none",
        "nan",
        "na",
        "null",
        "unknown",
        "not applicable",
        "p_value",
        "variable",
        "result",
        "exposure",
        "expression",
        "symbol"
      ).foreach { t =>
        assert(TermGates.isDeadTerm(t, cfg.deadTerms, cfg.compiledExcludeNameRegex))
      }
      // medically meaningless common words
      Seq("the", "and", "other", "miscellaneous").foreach { t =>
        assert(TermGates.isDeadTerm(t, cfg.deadTerms, cfg.compiledExcludeNameRegex))
      }
      assert(!TermGates.isDeadTerm("glucose", cfg.deadTerms, cfg.compiledExcludeNameRegex))
    }

    test("admitted returns the cleaned form or null") {
      assert(TermGates.admitted("  0.5  ", cfg) == null)
      assert(TermGates.admitted("\"unknown\"", cfg) == null) // quotes cleaned away, then dead
      assert(TermGates.admitted("English Language", cfg) == null)
      assert(TermGates.admitted("G6PC1", cfg) == "g6pc1")
      assert(
        TermGates.admitted("Glucose-6-Phosphatase Catalytic Subunit 1", cfg) ==
          "glucose-6-phosphatase catalytic subunit 1"
      )
    }

    test("taxon allowlist retention mirrors fullmap retain_for_taxon_allowlist") {
      val allow = Set("NCBITaxon:9606")
      // no allowlist -> keep everything
      assert(Filters.retainForTaxonAllowlist("biolink:Gene", Seq("NCBITaxon:1111111"), Set.empty))
      // OrganismTaxon rows are never filtered
      assert(Filters.retainForTaxonAllowlist("biolink:OrganismTaxon", Seq("NCBITaxon:1111111"), allow))
      // allowlisted taxon kept
      assert(Filters.retainForTaxonAllowlist("biolink:Gene", Seq("NCBITaxon:9606"), allow))
      // taxonless rows kept (incl. the NCBITaxon:0 sentinel — not a valid id)
      assert(Filters.retainForTaxonAllowlist("biolink:Gene", Seq.empty, allow))
      assert(Filters.retainForTaxonAllowlist("biolink:Gene", Seq("NCBITaxon:0"), allow))
      // all-valid-taxa outside the list -> dropped
      assert(!Filters.retainForTaxonAllowlist("biolink:Gene", Seq("NCBITaxon:1111111"), allow))
      // one allowlisted taxon among several suffices
      assert(Filters.retainForTaxonAllowlist("biolink:Gene", Seq("NCBITaxon:1111111", "NCBITaxon:9606"), allow))
    }
  }
}
