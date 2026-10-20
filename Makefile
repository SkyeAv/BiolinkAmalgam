.PHONY: setup build assembly test test-cluster bench run run-local fmt fmt-check lint fix check clean

# Resolve dependencies and compile everything in parallel.
setup:
	./mill -j 0 __.compile

# Build the local jar (Spark bundled) for `java -jar` / local[*] runs.
build:
	./mill biolinkamalgam.assembly

# Build the cluster jar (Spark provided) for spark-submit.
assembly:
	./mill cluster.assembly

# Run the test suite in parallel. Offline by construction: FixtureSnapshotProvider
# throws on any non-local path, so no test can download anything.
test:
	./mill -j 0 biolinkamalgam.test

# Start the local Spark master+worker, wait for registration, run ClusterSuite
# against spark://127.0.0.1:7077, then stop what we started (trap-guarded).
test-cluster:
	./scripts/test-cluster.sh

# Run the benchmark harness on the fixed small slice.
bench:
	./mill biolinkamalgam.run bench --out bench/history

# Run the pipeline locally: make run ARGS="run --babel <dir> --out out/"
run:
	./mill biolinkamalgam.run $(ARGS)

# Format all Scala sources.
fmt:
	./mill __.reformat

# Check formatting without changing files.
fmt-check:
	./mill __.checkFormat

# Lint: scalafix rules (.scalafix.conf) in check mode, then format check.
# Fails on any rule violation or unformatted file; changes nothing.
lint:
	./mill -j 0 __.fix --check
	./mill __.checkFormat

# Apply scalafix rewrites, then reformat.
fix:
	./mill -j 0 __.fix
	./mill __.reformat

# Full local quality gate: lint (scalafix + scalafmt) then the offline suite.
check: lint test

# Remove all build outputs.
clean:
	./mill clean
