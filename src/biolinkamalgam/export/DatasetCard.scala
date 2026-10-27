package biolinkamalgam.export

/** Hugging Face dataset card: README.md with YAML frontmatter, generated per run so the card always matches the parquet
  * it sits beside. Pure string generation (unit-testable, no Spark) plus a thin file writer.
  *
  * Upload flow: create a dataset repo on hf.co, drop in `biolinkamalgam.parquet` + this `README.md` — the hub's parquet
  * auto-conversion and viewer pick it up with no further config.
  */
object DatasetCard {

  /** HF size_categories bucket for a row count. */
  def sizeCategory(rows: Long): String =
    if (rows < 1000) "n<1K"
    else if (rows < 10000) "1K<n<10K"
    else if (rows < 100000) "10K<n<100K"
    else if (rows < 1000000) "100K<n<1M"
    else if (rows < 10000000) "1M<n<10M"
    else if (rows < 100000000) "10M<n<100M"
    else "100M<n<1B"

  /** Render the card. `meta` carries run provenance (babel snapshot version, custom sources, spark master) — unknown
    * values should be passed as "local"/"none", never fabricated.
    */
  def render(
      parquetFile: String,
      stages: Seq[Manifest.Stage],
      meta: Map[String, String],
      generated: String
  ): String = {
    val rows = stages.find(_.name == "arbitrate").map(_.rows).getOrElse(0L)
    val stageLines = stages
      .map(s => f"| ${s.name}%-10s | ${s.rows}%12d | ${s.millis}%8d ms |")
      .mkString("\n")
    val babel = meta.getOrElse("babel", "unknown")
    val gtdb = meta.getOrElse("gtdb", "none")
    val gtdbPara =
      if (gtdb == "none") ""
      else
        s"""|GTDB (Genome Taxonomy Database, `$gtdb`) contributes `biolink:OrganismTaxon` cliques:
          |every GTDB rank becomes a `GTDB:`-prefixed taxon CURIE, and species carrying an
          |`ncbi_species_taxid` crossref into their `NCBITaxon:` clique (visible as a `GTDB:`-prefixed
          |synonym; at most one GTDB species per NCBI species — many-to-one mappings stay singletons).
          |
          |""".stripMargin
    val clinvar = meta.getOrElse("clinvar", "none")
    val obo = meta.getOrElse("obo", "none")
    val extraPara =
      (if (clinvar == "none") ""
       else
         """|ClinVar `variant_summary` contributes `biolink:SequenceVariant` cliques: one `CLINVAR:` row per
            |variation and one `DBSNP:rs...` row per referenced rsID. An rsID can denote several alleles, so the
            |two are never merged; their links live in `crossrefs.parquet`.
            |
            |""".stripMargin) +
        (if (obo == "none") ""
         else
           """|Additional OBO ontologies not in BABEL (`config/obo_sources.yaml`: MP, ZP, FYPO, WBPhenotype,
              |APO, TO, MAXO, HsapDv, MmusDv, ZFS, FBdv, WBls, MOD, TAXRANK, FAO) contribute active own-prefix
              |terms with their names, EXACT synonyms and alt_ids; BROAD/NARROW/RELATED synonyms are excluded.
              |
              |""".stripMargin)
    val crossrefSection =
      if (clinvar == "none" && obo == "none") ""
      else
        """|
           |## crossrefs.parquet
           |
           |Typed associations, NOT equivalences: `subject, predicate, object, source` with predicates
           |`dbsnp_reference` (ClinVar variation -> rsID), `gene_association` (ClinVar variation -> NCBIGene),
           |`xref` (OBO term cross-reference; relevance only per OBO 1.4) and `replaced_by` (obsolete OBO term ->
           |successor).
           |""".stripMargin
    val extraLicense =
      (if (clinvar == "none") "" else "\nClinVar and dbSNP are NCBI public-domain data.") +
        (if (obo == "none") ""
         else
           "\nOBO ontologies carry CC-BY-3.0, CC-BY-4.0 or CC0-1.0 terms per ontology (see config/obo_sources.yaml).")

    s"""---
       |pretty_name: BiolinkAmalgam
       |configs:
       |  - config_name: default
       |    default: true
       |    data_files:
       |      - split: train
       |        path: biolinkamalgam.parquet
       |license: other
       |license_name: babel-upstream-licenses
       |task_categories:
       |  - other
       |tags:
       |  - biology
       |  - bioinformatics
       |  - entity-resolution
       |  - named-entity-recognition
       |  - biolink
       |  - synonyms
       |size_categories:
       |  - ${sizeCategory(rows)}
       |---
       |
       |# BiolinkAmalgam
       |
       |Biomedical entity synonym cliques built from [BABEL](https://github.com/TranslatorSRI/Babel)
       |compendia and synonyms (snapshot: `$babel`), merged and arbitrated so that
       |**within a (class, taxon scope), a synonym belongs to exactly one CURIE**.
       |${gtdbPara}${extraPara}Synonyms are the ORIGINAL strings with junk filtered out (see below); normalization
       |(stemming, level keys) is deliberately left to downstream consumers.
       |
       |## Schema
       |
       |One row per surviving clique leader.
       |
       || Column | Type | Meaning |
       ||---|---|---|
       || `CURIE` | string | clique leader identifier |
       || `PREFERRED_NAME` | string | the clique's preferred name, as written |
       || `CLASS` | string | Biolink category, e.g. `biolink:Gene` |
       || `SYNONYMS` | list<string> | synonyms as originally written, sorted; a synonym appears in at most one row per (class, taxon scope) |
       |$crossrefSection
       |## What was filtered
       |
       |- `biolink:Publication` and `biolink:Agent` cliques (titles and leftover-UMLS organizations).
       |- Taxa outside the top-100 experimental taxon allowlist (taxonless and `biolink:OrganismTaxon` cliques kept).
       |- Names that are pure numbers (including decimals like `0.5`), contain `language`,
       |  `uncharacterized` or `hypothetical`, or equal a medically meaningless stopword
       |  (`unknown`, `none`, `the`, ...; full list in `config/filters.yaml`).
       |- Conflated BABEL synonym files are never read: drug/chemical and gene/protein cliques stay distinct.
       |
       |## Generation
       |
       || stage | rows | time |
       ||---|---|---|
       |$stageLines
       |
       |Generated $generated by [BiolinkAmalgam](https://github.com/SkyeAv/BiolinkAmalgam).
       |
       |## Usage
       |
       |```python
       |from datasets import load_dataset
       |
       |ds = load_dataset("SkyeAv/biolink-amalgam", split="train")
       |row = ds[0]
       |print(row["CURIE"], row["PREFERRED_NAME"], row["CLASS"])
       |print(row["SYNONYMS"])
       |```
       |
       |## Licensing
       |
       |BABEL aggregates many upstream biomedical resources with heterogeneous licenses
       |(UMLS content in particular requires a UMLS license). This dataset inherits those
       |upstream terms; check the [BABEL license summary](https://github.com/TranslatorSRI/Babel)
       |for your intended use.${if (gtdb == "none") ""
      else "\nGTDB data are CC-BY-SA-4.0 (share-alike); GTDB-derived taxa carry those terms."}$extraLicense
       |""".stripMargin
  }

  /** Write README.md beside the parquet in `outDir`. `generated` defaults to today (UTC). */
  def write(
      outDir: os.Path,
      parquetFile: String,
      stages: Seq[Manifest.Stage],
      meta: Map[String, String]
  ): Unit = {
    val generated = java.time.LocalDate.now(java.time.ZoneOffset.UTC).toString
    os.write.over(outDir / "README.md", render(parquetFile, stages, meta, generated))
  }
}
