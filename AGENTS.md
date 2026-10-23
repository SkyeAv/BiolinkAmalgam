# BiolinkAmalgam

A Spark/Scala pipeline that turns BABEL outputs (plus your own ontology tables and
hand-curated manual entries) into a **single parquet file** publishable as a Hugging
Face dataset (`SkyeAv/biolink-amalgam`), with one hard invariant:

> Within a given **class × taxon scope**, a synonym belongs to exactly **one** CURIE.

The dataset ships **original strings** with junk filtered out. There is **no stemmer in
this codebase**: normalization (Porter2, level-one/level-two keys) is owned downstream by
Tablassert's fullmap. One NLP pipeline, one language — never two.

## Layout

- `src/biolinkamalgam/schema/Schemas.scala` — explicit StructTypes for every file shape
  read or written. Never use Spark schema inference.
- `src/biolinkamalgam/config/Config.scala` — loads `config/*.yaml` (taxa, filters,
  promoted prefixes, download, profiles) and owns taxon bucketing.
- `src/biolinkamalgam/ingest/` — `SnapshotProvider` (remote listing vs fixtures),
  `SnapshotFiles` (split-parts XOR whole-file selection, aria2 leftover exclusion),
  `BulkDownloader` (`aria2` default / `jdk-chunked` / `jdk`; one bulk invocation for the
  whole snapshot, HEAD-based skip of completed files).
- `src/biolinkamalgam/source/` — `SynonymSource` plugin base, BABEL compendia/synonyms
  readers, custom TSV/JSONL ontologies, manual `.ndjson` entries (BABEL PR #887 shape,
  priority 0 = overrides), `GtdbSource` (GTDB metadata TSVs, BABEL PR #978 port), and
  `Registry` (lists sources, unions + consolidates by clique leader; lowest-priority
  non-null preferred name wins).
- `src/biolinkamalgam/transform/` — `NameKey` (strip/clean only — no stemming),
  `TermGates` (junk admission gates from filters.yaml), `Filters` (category/prefix/taxon
  drops), `CliqueMerge` (preferred-name clique merging), `Uniqueness` (arbitration).
- `src/biolinkamalgam/export/` — `Parquet` (single-file binary concat), `Tsv`
  (header-aware concat), `Manifest` (run metrics), `DatasetCard` (HF README.md with
  YAML frontmatter, generated per run).
- `src/biolinkamalgam/Pipeline.scala` + `Main.scala` — stage orchestration + mainargs
  CLI (`run`, `bench`, `benchDownload`).
- `config/` — `taxa.yaml`, `filters.yaml`, `promote_prefixes.yaml`, `download.yaml`,
  `profiles.yaml`. Big, reviewable decisions live here, not in code. Every entry carries
  a why-comment. **YAML gotcha: quote bare `null`/`true`/`false` scalars in lists** (a
  plain `- null` parses to a null element, not the string).
- `test/` — utest suites + fixture snapshot (`test/resources/snapshots/mini`), custom
  ontology (`test/resources/custom/mini.tsv`), manual entries (`manual.ndjson`).

## The canonical output

One file, `biolinkamalgam.parquet`, schema exactly:

```
CURIE: string, PREFERRED_NAME: string, CLASS: string, SYNONYMS: array<string>
```

One row per surviving clique leader. A clique whose names all lost arbitration (or failed
the gates) still gets a row with empty SYNONYMS. Beside it: `run_manifest.json` and a
generated `README.md` HF dataset card. `conflicts.tsv` + `merges.tsv` (full audit) are
opt-in via `--audit` — at BABEL scale they are hundreds of GB and unwritable in practice.

## Invariants and rules (do not break)

- **No conflation, ever.** `synonyms-conflated/` and `*Conflated*` files are excluded in
  both the remote listing and the synonym reader. Drug/chemical and gene/protein cliques
  stay distinct (unlike Tablassert's fullmap).
- **Taxon bucketing.** Only taxa in `config/taxa.yaml` `major_taxa` get their own scope;
  the long tail shares `__OTHER_TAXA__`, no-taxon cliques share `__NO_TAXON__`. The same
  name MAY recur across categories or taxa.
- **Taxon allowlist** (`taxa.yaml` `taxon_allowlist`, Tablassert fullmap
  `retain_for_taxon_allowlist` parity): when non-empty, cliques whose valid taxa
  (positive NCBITaxon ids) are all outside the list are dropped at ingest; taxonless and
  biolink:OrganismTaxon cliques are always kept.
- **No stemming.** Merge and arbitration keys are strip-only (trim, lowercase, drop
  non-word chars, collapse whitespace). Inflections ("glucose"/"glucoses") stay distinct.
  Output SYNONYMS always keep ORIGINAL strings.
- **Junk gates are filters, not NLP** (`TermGates` on the quote-stripped lowercase
  cleaned form): banned substrings (`language`, `uncharacterized`, `hypothetical`),
  exact-match dead terms, numeric junk incl. decimals (`0.5`, `1.05` — these acquire
  garbage UMLS CURIEs upstream). All lists live in `config/filters.yaml`.
- **Winner order** (merge and arbitration): preferred-name match → most synonyms →
  identifier count → source priority (lower wins; manual/custom 0, BABEL 10) → smallest
  CURIE. Total order, so reruns reproduce.
- **INCHIKEY promotion**: identifier CURIEs whose prefix is in
  `config/promote_prefixes.yaml` become synonyms of their clique (fullmap never did this).
- **Dropped by default** (filters.yaml): `biolink:Publication` (PMID/doi/PMC titles) and
  `biolink:Agent` (BABEL carries it only as leftover-UMLS organization cliques — no
  ROR/GRID/ORCID/ISNI/VIAF exist in BABEL, verified against the 2025mar31/2026jul22
  snapshots; `drop_curie_prefixes_by_category` is the mechanism if a future source mixes
  persons and institutes).
- **Pure DataFrame transforms**: CliqueMerge/Uniqueness take and return DataFrames; no
  driver-side collection of clique data.
- **Tests are offline by construction**: `FixtureSnapshotProvider` throws on non-local
  paths. Never add a test that needs the network; DownloadSuite uses an in-JVM
  `com.sun.net.httpserver` fake (live aria2 test self-skips when aria2c is absent).
- **Fixture fidelity**: `test/resources/snapshots/mini/compendia/Gene.txt` line 1 is the
  verbatim G6PC1 clique from BABEL's docs/DataFormats.md. Keep fixture rows faithful to
  real BABEL shapes; test copies of configs live in `test/resources/config/` (keep in
  sync with `config/`).
- **No effect systems** (no cats-effect/ZIO) — JDK concurrency only.
- Scaladoc comments must not contain `*/` — write `compendia/ .txt`-style paths as
  `` `.txt` `` instead of `/*.txt`.

## Build & test

Mill 1.x, Scala 2.13.16, Spark 4.0.1. Mill defaults to `<module>/src`; this repo keeps
the flat `src/` + `test/` layout via `moduleDir` overrides in `build.mill` — don't "fix"
that by moving directories.

- `make setup` — resolve deps, compile everything
- `make test` — offline test suite (parallel forked JVMs)
- `make lint` / `make fix` — scalafix (OrganizeImports, RemoveUnused, DisableSyntax) +
  scalafmt check / apply
- `make check` — lint + tests (the quality gate)
- `make test-cluster` — local standalone Spark master+worker, sets
  `BIOLINKAMALGAM_SPARK_MASTER` (activates ClusterSuite)

**This laptop (30GB) cannot run full builds or heavy test shards** — run them on
wenceslaus (80 cores / 187GB; mill, Spark 4.0.1, aria2 are in its home-manager profile;
`rebuild` = `home-manager switch --flake /local_raid1/sgoetz/home-manager#sgoetz`).
Long remote jobs go in tmux (the jump path idle-kills SSH masters). Dev loop: edit
locally, `rsync -az --delete --exclude out/ --exclude .git/ --exclude data/`
to `/local_raid1/sgoetz/Code/ISB/BiolinkAmalgam/`, run the gate in tmux.

## Run

- `make run ARGS="run --babel <dir> --out out/run1"` — pipeline against a local snapshot
- `make run ARGS="run --download 2026jul22 --snapshot-dir <dir> --profile wenceslaus --out out/run1"` —
  bulk-download a BABEL snapshot first (aria2, resumable; NOT into out/, mill wipes it)
- `make run ARGS="run --babel <dir> --sample 200000 --out out/sample"` — fast iteration:
  caps raw rows per source before parsing
- `make run ARGS="run --babel <dir> --ingest-cache data/ingest-cache --out out/run1"` —
  materialize the consolidated clique table once; reruns with the same snapshot files +
  ingest-affecting config (taxa allowlist, promoted prefixes, category/prefix drops,
  sample size) read the parquet cache in seconds. Name-gate edits in `filters.yaml`
  live in the arbitrate stage and never invalidate the cache, so the loop
  "edit a gate/stopword list -> rerun" costs minutes, not an hour.
- `make run ARGS="run --download 2026jul22 --snapshot-dir <dir> --gtdb <dir> --out out/run1"` —
  include GTDB taxon cliques (drop GTDB `bac120`/`ar53` metadata `.tsv[.gz]` files into
  `<dir>`; read by column name, so reduced-column files are fine)
- `make run ARGS="validate --out <run-dir> --profile wenceslaus"` — pre-publication
  sanity checks (schema, CURIE uniqueness, one-CURIE-per-(class, synonym), dropped
  classes/gates actually absent); exit 1 on any failure
- `make run ARGS="benchDownload --url <big-file-url> --out /tmp/dlbench"` — time aria2 vs
  jdk-chunked vs jdk on a real file (evidence for `config/download.yaml`)

## Adding your own data

Drop files into a directory and pass `--custom <dir>`:

- `.tsv` — `curie\tlabel\tsynonyms\tcategory\ttaxa` (pipe-separated lists)
- `.jsonl` — `Schemas.custom` rows
- `.ndjson` — manual entries (BABEL PR #887 shape: `curie`, `type`, `preferred`,
  `alternatives`, optional `equivalents` for multi-CURIE cliques, optional `taxa`).
  Priority 0 — manual entries override BABEL (preferred name + arbitration tiebreaks).

Custom sources get priority 0 and outrank BABEL in arbitration. For anything more
complex, subclass `SynonymSource` and add one line to `Registry.forConfig`.

GTDB (BABEL PR #978 port, `--gtdb <dir>`): every `gtdb_taxonomy` rank becomes a
`GTDB:`-prefixed `biolink:OrganismTaxon` clique (`GTDB:s__Escherichia_coli`, display
name "Escherichia coli"). Species with an `ncbi_species_taxid` crossref into their
`NCBITaxon:` clique as an identifier — AT MOST ONE GTDB species per NCBI species taxid
(alphabetically first wins); the rest stay GTDB singletons, because two GTDB species
sharing one taxid must never merge.
