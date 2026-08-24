# Amalgam

A Spark/Scala pipeline that turns BABEL outputs (plus your own ontology tables) into a
**single parquet file** for fullmap-DB ingestion, with one hard invariant:

> Within a given **category × taxon scope**, a synonym belongs to exactly **one** CURIE.

## Layout

- `src/amalgam/schema/Schemas.scala` — explicit StructTypes for every file shape read or
  written. Never use Spark schema inference.
- `src/amalgam/config/Config.scala` — loads `config/*.yaml` (major taxa, promoted CURIE
  prefixes, resource profiles) and owns taxon bucketing.
- `src/amalgam/ingest/SnapshotProvider.scala` — the download seam. `RemoteSnapshotProvider`
  (async JDK HttpClient, Range resume, atomic rename) vs `FixtureSnapshotProvider`.
- `src/amalgam/source/` — `SynonymSource` plugin base, BABEL compendia/synonyms readers,
  custom TSV/JSONL ontologies, and `Registry` (lists sources, unions + consolidates by
  clique leader).
- `src/amalgam/transform/` — `NameKey` (match keys), `CliqueMerge` (preferred-name clique
  merging), `Uniqueness` (synonym arbitration).
- `src/amalgam/export/` — `Parquet` (single-file binary concat), `Tsv` (header-aware
  concat), `Manifest` (run metrics).
- `src/amalgam/Pipeline.scala` + `Main.scala` — stage orchestration + mainargs CLI
  (`run`, `bench`).
- `config/` — `taxa.yaml`, `promote_prefixes.yaml`, `profiles.yaml`. Big, reviewable
  decisions live here, not in code. Every entry carries a why-comment.
- `test/` — utest suites + fixture snapshot (`test/resources/snapshots/mini`) and custom
  ontology (`test/resources/custom/mini.tsv`).

## The canonical output

One file, `amalgam.parquet`, schema exactly:

```
CURIE: string, PREFERRED_NAME: string, CATEGORY: string, SYNONYMS: array<string>
```

One row per surviving clique leader. A clique whose names all lost arbitration still
gets a row with empty SYNONYMS. Audit reports `conflicts.tsv` and `merges.tsv` sit
beside it, plus `run_manifest.json`.

## Invariants and rules (do not break)

- **No conflation, ever.** `synonyms-conflated/` and `*Conflated*` files are excluded in
  both the remote listing and the synonym reader. Drug/chemical and gene/protein cliques
  stay distinct (unlike Tablassert's fullmap).
- **Taxon bucketing.** Only taxa in `config/taxa.yaml` get their own scope; the long tail
  shares `__OTHER_TAXA__`, no-taxon cliques share `__NO_TAXON__`. Clique merging and
  arbitration are scoped by (category, taxon bucket) — the same name MAY recur across
  categories or taxa.
- **Preferred names are never stemmed** — clique merge matches on strip-only keys (trim,
  lowercase, drop non-word chars, collapse whitespace). Synonym arbitration stems
  (Snowball Porter via lucene-analysis-common — Lucene's own `PorterStemmer` is
  package-private, don't try to import it). Output SYNONYMS always keep ORIGINAL strings.
- **Winner order** (merge and arbitration): most synonyms → identifier count → source
  priority (lower wins; custom ontologies are 0, BABEL 10) → smallest CURIE. Total order,
  so reruns reproduce.
- **INCHIKEY promotion**: identifier CURIEs whose prefix is in
  `config/promote_prefixes.yaml` become synonyms of their clique (fullmap never did this).
- **Pure DataFrame transforms**: CliqueMerge/Uniqueness take and return DataFrames; no
  driver-side collection of clique data.
- **Tests are offline by construction**: `FixtureSnapshotProvider` throws on non-local
  paths. Never add a test that needs the network; DownloadSuite uses an in-JVM
  `com.sun.net.httpserver` fake instead.
- **Fixture fidelity**: `test/resources/snapshots/mini/compendia/Gene.txt` line 1 is the
  verbatim G6PC1 clique from BABEL's docs/DataFormats.md. Keep fixture rows faithful to
  real BABEL shapes; test copies of configs live in `test/resources/config/` (keep in
  sync with `config/`).
- **No effect systems** (no cats-effect/ZIO) — JDK concurrency only.
- Scaladoc comments must not contain `*/` — write `compendia/ .txt`-style paths as
  `` `.txt` `` instead of `/*.txt`.

## Build & run

Mill 1.x, Scala 2.13.16, Spark 4.0.1 (matches the system Spark). Mill defaults to
`<module>/src`; this repo keeps the flat `src/` + `test/` layout via `moduleDir`
overrides in `build.mill` — don't "fix" that by moving directories.

- `make setup` — resolve deps, compile everything
- `make test` — offline test suite (parallel forked JVMs)
- `make test-cluster` — starts the local standalone Spark master+worker
  (`spark://127.0.0.1:7077`), builds the Spark-provided `cluster` assembly, runs the
  suite with `AMALGAM_SPARK_MASTER` set (this activates `ClusterSuite`)
- `make run ARGS="run --babel <dir> --out out/run1"` — pipeline against a local snapshot
- `make run ARGS="run --download 2025mar31 --out out/run1"` — download a BABEL snapshot
  first (resumable)
- `make fmt` / `make check` — scalafmt, then fmt-check + tests (the quality gate)

This box: 16 CPUs / 20 GB. The `local` profile in `config/profiles.yaml` (cores.max=14,
executor 10g, driver 4g, shuffle.partitions=56) is sized for it.

## Adding your own ontology

Drop a `.tsv` (`curie\tlabel\tsynonyms\tcategory\ttaxa`, pipe-separated lists) or
`.jsonl` (`Schemas.custom`) file into a directory and pass `--custom <dir>`. Custom
sources get priority 0 and outrank BABEL in arbitration. For anything more complex,
subclass `SynonymSource` and add one line to `Registry.forConfig`.
