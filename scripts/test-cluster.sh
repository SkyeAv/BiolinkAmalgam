#!/usr/bin/env sh
# test-cluster: start the local standalone Spark master + worker, wait for the
# worker to register, run the test suite against spark://127.0.0.1:7077, then
# stop whatever we started. Trap-guarded so cleanup runs on failure too.
set -eu

MASTER=spark://127.0.0.1:7077
MASTER_UI=http://127.0.0.1:8180
TIMEOUT=60

started_master=0
started_worker=0

cleanup() {
  if [ "$started_worker" = 1 ]; then
    systemctl stop spark-worker || true
  fi
  if [ "$started_master" = 1 ]; then
    systemctl stop spark-master || true
  fi
  # Remove Spark work/scratch dirs created under the default worker work dir.
  rm -rf "${TMPDIR:-/tmp}"/spark-* 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# Start the master only if nothing is listening on 7077 already.
if ! (exec 3<>/dev/tcp/127.0.0.1/7077) 2>/dev/null; then
  echo "Starting spark-master (may prompt for auth)..."
  systemctl start spark-master
  started_master=1
fi

# Start the worker only if not already active.
if ! systemctl is-active --quiet spark-worker; then
  echo "Starting spark-worker (may prompt for auth)..."
  systemctl start spark-worker
  started_worker=1
fi

# Wait for the worker to register with the master.
echo "Waiting for worker registration (up to ${TIMEOUT}s)..."
i=0
while [ "$i" -lt "$TIMEOUT" ]; do
  if curl -sf "$MASTER_UI/json/" 2>/dev/null | grep -qE '"aliveworkers" *: *[1-9]'; then
    echo "Worker registered."
    break
  fi
  i=$((i + 1))
  sleep 1
done
if [ "$i" -ge "$TIMEOUT" ]; then
  echo "Timed out waiting for worker registration. Recent logs:" >&2
  systemctl status spark-worker --no-pager || true
  exit 1
fi

# The executors need the app classes (lucene stemmer UDFs included) but not Spark
# itself — the worker provides that. Build the Spark-provided cluster assembly.
./mill cluster.assembly
ASSEMBLY_JAR="$(pwd)/out/cluster/assembly.dest/out.jar"

BIOLINKAMALGAM_SPARK_MASTER="$MASTER" BIOLINKAMALGAM_CLUSTER_JAR="$ASSEMBLY_JAR" ./mill -j 0 biolinkamalgam.test
