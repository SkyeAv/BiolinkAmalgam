package biolinkamalgam.export

/** Hugging Face dataset card (`README.md`, YAML frontmatter) and data license (`LICENSE.md`), generated per run so both
  * always match the files beside them. Pure string generation (unit-testable, no Spark) plus a thin file writer.
  *
  * Hub layout: two subsets. `cliques` has one split per Biolink class (`cliques/<split>.parquet`); `crossrefs` has one
  * split per edge source (`crossrefs/<split>.parquet`). Split names come from [[Splits]].
  */
object DatasetCard {

  val Repo = "SkyeAv/amalgam"

  /** One split: its hub name, the value it was derived from (a Biolink class or a crossref source) and its row count.
    */
  case class Split(name: String, value: String, rows: Long) {
    require(Splits.isValid(name), s"invalid split name: $name")
    require(rows >= 0, s"negative row count for $name")
  }

  /** HF size_categories bucket for a row count. */
  def sizeCategory(rows: Long): String =
    if (rows < 1000) "n<1K"
    else if (rows < 10000) "1K<n<10K"
    else if (rows < 100000) "10K<n<100K"
    else if (rows < 1000000) "100K<n<1M"
    else if (rows < 10000000) "1M<n<10M"
    else if (rows < 100000000) "10M<n<100M"
    else if (rows < 1000000000) "100M<n<1B"
    else "1B<n<10B"

  private val PredicateMeaning = Map(
    "dbsnp_reference" -> "ClinVar variation to the dbSNP rsID it references (one rsID can cover several alleles)",
    "gene_association" -> "variant to the NCBIGene it lies in (location, not identity)",
    "xref" -> "OBO term cross-reference (relevance only, per OBO 1.4)",
    "replaced_by" -> "obsolete OBO term to its successor",
    "cites" -> "dbSNP locus to a PubMed citation",
    "merged_into" -> "retired dbSNP rsID to the rsID it was merged into"
  )

  private def splitConfig(subset: String, splits: Seq[Split], default: Boolean): String =
    if (splits.isEmpty) ""
    else {
      val files = splits
        .map(s => s"""|      - split: ${s.name}
                                      |        path: $subset/${s.name}.parquet
                                      |""".stripMargin)
        .mkString
      s"""|  - config_name: $subset
          |${if (default) "    default: true\n" else ""}    data_files:
          |$files""".stripMargin
    }

  private def splitTable(header: String, splits: Seq[Split]): String =
    (s"| split | $header | rows |\n|---|---|---:|\n" +
      splits.map(s => s"| `${s.name}` | `${s.value}` | ${s.rows} |").mkString("\n"))

  private def validated(splits: Seq[Split], subset: String): Seq[Split] = {
    val dup = splits.groupBy(_.name).collect { case (n, ss) if ss.size > 1 => n }
    require(dup.isEmpty, s"duplicate $subset split names: ${dup.mkString(",")}")
    splits.sortBy(_.name)
  }

  /** Render README.md. `meta` carries run provenance (babel snapshot, gtdb, clinvar, obo, taxon_allowlist); unknown
    * values must be passed as "none", never fabricated.
    */
  def render(
      cliqueSplits: Seq[Split],
      crossrefSplits: Seq[Split],
      predicates: Seq[String],
      meta: Map[String, String],
      generated: String
  ): String = {
    predicates.foreach(p => require(Splits.isValid(p), s"crossref predicate must be a lowercase identifier: $p"))
    val cliques = validated(cliqueSplits, "cliques")
    val crossrefs = validated(crossrefSplits, "crossrefs")
    val totalCliques = cliques.map(_.rows).sum
    val babel = meta.getOrElse("babel", "none")
    def has(key: String) = meta.getOrElse(key, "none") != "none"
    val allowlist = meta.getOrElse("taxon_allowlist", "0")
    val taxaNote =
      if (allowlist == "0") ""
      else
        s"- Except `biolink:OrganismTaxon`, rows with taxa keep at least one of the $allowlist allowlisted taxa (`config/taxa.yaml`)." +
          (if (!has("taxon_inference")) "\n"
           else
             """ Taxonless rows first
               |  inherit the taxon of single-species identifiers (MGI, RGD, ZFIN, FlyBase, WormBase, SGD, HGNC, Ensembl).
               |""".stripMargin)
    val promoted = meta.getOrElse("promoted_prefixes", "").split(",").filter(_.nonEmpty).toSeq
    val promotedNote =
      if (promoted.contains("*")) " Leader CURIEs and equivalent identifiers are indexed as synonyms (fullmap parity)."
      else if (promoted.isEmpty) ""
      else s" ${promoted.map(p => s"`$p:`").mkString(", ")} identifiers are added as synonyms."

    val extraSources = Seq(
      Option.when(has("gtdb"))(
        "- [GTDB](https://gtdb.ecogenomic.org/): `GTDB:` taxa (`organism_taxon`); species carry their NCBI " +
          "species taxid crossref as a `GTDB:` synonym of the `NCBITaxon:` row (at most one GTDB species per taxid)."
      ),
      Option.when(has("clinvar"))(
        "- [ClinVar](https://www.ncbi.nlm.nih.gov/clinvar/) `variant_summary`: one `CLINVAR:` row per variation and " +
          "one `DBSNP:rs...` row per referenced rsID (`sequence_variant`), never merged with each other."
      ),
      Option.when(has("obo"))(
        "- OBO ontologies not in BABEL (`config/obo_sources.yaml`): active own-prefix terms with names, EXACT " +
          "synonyms and alt_ids."
      )
    ).flatten
    val sourcesPara =
      if (extraSources.isEmpty) ""
      else s"\nAdditional sources:\n\n${extraSources.mkString("\n")}\n"

    val usageCrossrefs =
      crossrefs.headOption
        .map { s =>
          val streaming = if (s.rows == 0) ", streaming=True" else ""
          s"""\nedges = load_dataset("$Repo", "crossrefs", split="${s.name}"$streaming)"""
        }
        .getOrElse("")
    val firstClique = cliques.find(_.name == "gene").orElse(cliques.headOption).map(_.name).getOrElse("gene")

    val crossrefSection =
      if (crossrefs.isEmpty) ""
      else {
        val preds = predicates.distinct.sorted
          .map(p => s"- `$p`: ${PredicateMeaning.getOrElse(p, "typed edge")}")
          .mkString("\n")
        s"""|
            |### `crossrefs`
            |
            |Typed associations, not equivalence. One split per source; source lives in the split name, not a column.
            |Edges retain pre-merge IDs and may refer to rows absent from `cliques`. A zero-row `empty` split is emitted
            |when no source contributes edges. Read that empty split with `streaming=True`; the datasets library
            |rejects zero-row splits in materialized mode.
            |
            || column | type | meaning |
            ||---|---|---|
            || `SUBJECT` | string | source CURIE (pre-merge identifier, may be obsolete) |
            || `PREDICATE` | string | edge type, below |
            || `OBJECT` | string | target CURIE |
            |
            |${splitTable("source", crossrefs)}
            |
            |$preds
            |""".stripMargin
      }

    s"""---
       |pretty_name: Amalgam
       |license: other
       |license_name: polyform-noncommercial-1.0.0
       |license_link: LICENSE.md
       |configs:
       |${splitConfig("cliques", cliques, default = true)}${splitConfig(
        "crossrefs",
        crossrefs,
        default = false
      )}task_categories:
       |  - other
       |tags:
       |  - biology
       |  - biomedical
       |  - entity-linking
       |  - synonyms
       |  - biolink
       |size_categories:
       |  - ${sizeCategory(totalCliques)}
       |---
       |
       |# Amalgam
       |
       |Amalgam is a biomedical metaontology: a cross-resource integration layer of entity identifiers, names,
       |and typed cross-references organized by Biolink class. It integrates source ontologies rather than replacing
       |their semantics; typed associations are not equivalence assertions.
       |
       |Biomedical entity names for [Biolink](https://biolink.github.io/biolink-model/) classes, built from
       |[BABEL](https://github.com/NCATSTranslator/Babel) snapshot `$babel`. Cliques that share a preferred name
       |within a class and taxon scope may merge unless identifier namespaces conflict. Every synonym is assigned to one CURIE within its
       |class and taxon scope. Strings are kept as written; normalization is left to consumers.
       |$sourcesPara
       |## Subsets
       |
       |### `cliques` (default)
       |
       |One row per clique, one split per Biolink class.
       |
       || column | type | meaning |
       ||---|---|---|
       || `CURIE` | string | clique leader identifier |
       || `PREFERRED_NAME` | string | preferred name, as written |
       || `CLASS` | string | Biolink class, e.g. `biolink:Gene` (split `gene`) |
       || `SYNONYMS` | list<string> | sorted names; may be empty when every name lost arbitration |
       || `TAXA` | list<string> | sorted `NCBITaxon:` CURIEs; empty when taxonless |
       |
       |${splitTable("class", cliques)}
       |$crossrefSection
       |## Usage
       |
       |```python
       |from datasets import load_dataset
       |
       |genes = load_dataset("$Repo", "cliques", split="$firstClique")
       |cliques = load_dataset("$Repo", "cliques")  # DatasetDict, one entry per class$usageCrossrefs
       |```
       |
       |## Construction
       |
       |- Synonyms are unique per (class, taxon scope); the same string may recur across classes or taxa.
       |  Ties go to: preferred-name match, most synonyms, most identifiers, curated sources, smallest CURIE.
       |- Conflated BABEL files are never read; drugs and chemicals, genes and proteins stay distinct.
       |- A CURIE-like preferred name uses the first sorted, admitted non-CURIE synonym when available.
       |  Existing human-readable preferred names are preserved.
       |- `biolink:Publication` and `biolink:Agent` are dropped.
       |$taxaNote
       |- Junk names are removed: numbers, `uncharacterized`, `hypothetical`, `language`, and stopwords
       |  (`config/filters.yaml`).$promotedNote
       |
       |Generated $generated by [Amalgam](https://github.com/SkyeAv/Amalgam).
       |
       |## License
       |
       |Amalgam code and original compilation rights are licensed under PolyForm Noncommercial 1.0.0.
       |This does not relicense upstream data or restrict rights granted independently by upstream licensors.
       |No single license covers all underlying content: each name keeps its upstream terms, and several
       |(UMLS, OMIM, DrugBank, KEGG, Orphanet, HMDB) restrict redistribution or commercial use. See
       |[LICENSE.md](LICENSE.md) before use.
       |""".stripMargin
  }

  /** Render LICENSE.md: upstream terms for every source this run included. `oboLicenses` maps OBO prefix to license. */
  def license(meta: Map[String, String], oboLicenses: Map[String, String] = Map.empty): String = {
    def has(key: String) = meta.getOrElse(key, "none") != "none"
    val extra = Seq(
      Option.when(has("gtdb"))("| GTDB | CC-BY-SA-4.0 | share-alike | https://gtdb.ecogenomic.org/about |"),
      Option.when(has("clinvar"))(
        "| ClinVar, dbSNP | public domain (NCBI) | none; cite NCBI | https://www.ncbi.nlm.nih.gov/home/about/policies/ |"
      )
    ).flatten ++ oboLicenses.toSeq.sorted.map { case (p, l) =>
      s"| $p (OBO) | $l | per license | https://obofoundry.org/ontology/${p.toLowerCase(java.util.Locale.ROOT)}.html |"
    }
    s"""# Amalgam data license
       |
       |Amalgam redistributes identifiers, names and synonyms compiled by
       |[BABEL](https://github.com/NCATSTranslator/Babel) and other sources.
       |
       |Amalgam code and original compilation rights (selection and arrangement, to the extent protectable)
       |are licensed under [PolyForm Noncommercial 1.0.0](https://polyformproject.org/licenses/noncommercial/1.0.0).
       |The license text is at https://github.com/SkyeAv/Amalgam/blob/main/LICENSE.
       |For that grant, the licensed work includes Amalgam's original compilation contributions.
       |Upstream content is not relicensed: every record retains its upstream terms and attribution requirements.
       |This grant does not limit rights available independently under upstream licenses or public-domain status,
       |and it grants no permissions that upstream licensors withhold. BABEL's code remains MIT.
       |Previously distributed Apache-2.0 versions retain their existing grants.
       |
       |A CURIE prefix identifies the leader's namespace, not the provenance of every synonym. Merged cliques can
       |carry names from several resources; per-name source provenance is not exported. Removing restricted-prefix
       |rows alone does not establish license compliance.
       |
       |## Restricted sources
       |
       |These may require an agreement or limit redistribution or commercial use. Check the terms for the exact
       |upstream release and content used. A notice does not grant redistribution rights; public distribution must
       |be authorized independently.
       |
       || source | terms | restriction | URL |
       ||---|---|---|---|
       || UMLS Metathesaurus (incl. SNOMED CT, RxNorm, MedDRA content) | UMLS Metathesaurus License | signed agreement; per-vocabulary restrictions; SNOMED CT affiliate terms | https://www.nlm.nih.gov/research/umls/knowledge_sources/metathesaurus/release/license_agreement.html |
       || OMIM | Johns Hopkins copyright | no redistribution; commercial use needs a license | https://omim.org/help/copyright |
       || DrugBank | DrugBank terms | non-commercial academic use; no redistribution | https://trust.drugbank.com/drugbank-trust-center/drugbank-terms-of-use |
       || KEGG | Kanehisa Laboratories | non-academic use needs a commercial license | https://www.kegg.jp/kegg/legal.html |
       || Orphanet | Orphadata terms | data transfer agreement | https://www.orphadata.com/faq/ |
       || HMDB, SMPDB | custom | commercial use or redistribution needs permission | https://hmdb.ca/downloads |
       |
       |## Open sources
       |
       || source | terms | condition | URL |
       ||---|---|---|---|
       || NCBI Gene, NCBI Taxonomy, PubChem, MeSH | public domain (NLM/NCBI) | cite NCBI/NLM | https://www.ncbi.nlm.nih.gov/home/about/policies/ |
       || HGNC | CC0-1.0 | none | https://www.genenames.org/about/license/ |
       || Reactome | CC0-1.0 (data) | none | https://reactome.org/license |
       || DOID | CC0-1.0 | none | https://obofoundry.org/ontology/doid.html |
       || ChEBI | CC-BY-4.0 | attribution | https://www.ebi.ac.uk/chebi/ |
       || UniProtKB | CC-BY-4.0 | attribution | https://www.uniprot.org/help/license |
       || Rhea | CC-BY-4.0 | attribution | https://www.rhea-db.org/help/license-disclaimer |
       || NCIt | CC-BY-4.0 | attribution | https://obofoundry.org/ontology/ncit.html |
       || GO, MONDO | CC-BY-4.0 | attribution | https://obofoundry.org/ontology/go.html |
       || UBERON | CC-BY-3.0 | attribution | https://obofoundry.org/ontology/uberon.html |
       || FlyBase | CC-BY-4.0 | attribution | https://wiki.flybase.org/wiki/FlyBase:Citing_FlyBase |
       || ChEMBL | CC-BY-SA-3.0 | attribution, share-alike | https://chembl.gitbook.io/chembl-interface-documentation/about |
       || DrugCentral | CC-BY-SA-4.0 | attribution, share-alike | https://drugcentral.org/privacy |
       || Guide to Pharmacology | ODbL-1.0 / CC-BY-SA-4.0 | attribution, share-alike | https://www.guidetopharmacology.org/about.jsp |
       |${extra.mkString("\n")}
       |
       |Other BABEL inputs (including Ensembl, MGI, RGD, ZFIN, SGD, WormBase, HP, EFO, ENZYME, PANTHER, UniChem, UNII,
       |Complex Portal, CLO, Wikidata and further OBO ontologies) are governed by their own published terms; check them
       |for your use. This summary is informational, not legal advice, and upstream terms change; the linked terms
       |prevail.
       |""".stripMargin
  }

  /** Write README.md and LICENSE.md into `outDir`. `generated` defaults to today (UTC). */
  def write(
      outDir: os.Path,
      cliqueSplits: Seq[Split],
      crossrefSplits: Seq[Split],
      predicates: Seq[String],
      meta: Map[String, String],
      oboLicenses: Map[String, String] = Map.empty
  ): Unit = {
    val generated = java.time.LocalDate.now(java.time.ZoneOffset.UTC).toString
    os.write.over(outDir / "README.md", render(cliqueSplits, crossrefSplits, predicates, meta, generated))
    os.write.over(outDir / "LICENSE.md", license(meta, oboLicenses))
  }
}
