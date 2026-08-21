#!/usr/bin/env bash
# Runs a freshly built image and asserts it does what its tag claims.
#
# Why this exists: the 0.1.2 image was published broken and nobody noticed,
# because the pipeline's only success criterion was "the build completed".
# It exited 1 on start with "ModuleNotFoundError: No module named 'fastapi'"
# -- the ingest extra was never installed -- and reported version 0.1.0
# under the 0.1.2 tag. Both are things a single `docker run` would have
# caught, and no step ever ran the image.
#
# Kept as a script rather than inline workflow steps so it can be run
# against a local build:
#
#   docker build --build-arg TH2PULSE_VERSION=0.1.3 -t th2pulse:smoke .
#   IMAGE=th2pulse:smoke EXPECTED_VERSION=0.1.3 .github/scripts/smoke-test-image.sh
#
# Required env vars:
#   IMAGE             - image reference to test, e.g. th2pulse:smoke
#   EXPECTED_VERSION  - version the image is about to be published as
# Optional env vars:
#   DB_DSN            - if set, also starts the service against it and
#                       requires /healthz to answer 200
set -euo pipefail

: "${IMAGE:?IMAGE is required}"
: "${EXPECTED_VERSION:?EXPECTED_VERSION is required}"

fail() { echo "::error::$*" >&2; exit 1; }

# 1. The CMD's imports resolve, and the version matches the tag. --network
#    none is not decoration: it is the assertion that nothing is fetched at
#    container start. The 0.1.2 image ran `uv run`, which re-resolved the
#    environment on every start and pulled the dev group over the network.
echo "--- imports and version, with no network ---"
reported=$(docker run --rm --network none "$IMAGE" python -c \
  'import importlib.metadata, fastapi, uvicorn, asyncpg; print(importlib.metadata.version("th2pulse"))') \
  || fail "the image cannot import what its CMD needs, or needs network access to start"
[ "$reported" = "$EXPECTED_VERSION" ] \
  || fail "image reports th2pulse ${reported} but is about to be published as ${EXPECTED_VERSION}"
echo "reports ${reported}, imports resolve offline."

# 2. The image binds 0.0.0.0, so it must refuse to serve unauthenticated.
#    Asserting on the refusal, not just on a non-zero exit: "it crashed" and
#    "it declined" are different outcomes and only one of them is correct.
echo "--- refuses to serve unauthenticated ---"
refusal=$(docker run --rm --network none \
  -e TH2PULSE_DB_DSN="postgresql://unused@127.0.0.1:5432/unused" \
  "$IMAGE" 2>&1 || true)
grep -q "Refusing to start" <<< "$refusal" \
  || fail "image did not refuse to serve unauthenticated on its 0.0.0.0 bind. Got: ${refusal}"
echo "refuses, and says how to proceed."

# 3. With a database and both tokens, it actually serves.
if [ -z "${DB_DSN:-}" ]; then
  echo "--- DB_DSN unset, skipping the serving check ---"
  exit 0
fi

echo "--- serves /healthz once configured ---"
container=$(docker run -d --network host \
  -e TH2PULSE_DB_DSN="$DB_DSN" \
  -e TH2PULSE_INGEST_TOKEN=smoke -e TH2PULSE_QUERY_TOKEN=smoke \
  "$IMAGE")
trap 'docker rm -f "$container" >/dev/null 2>&1 || true' EXIT

for attempt in $(seq 1 30); do
  if code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 http://127.0.0.1:4319/healthz) \
     && [ "$code" = "200" ]; then
    echo "/healthz answered 200 (attempt ${attempt}/30)."
    exit 0
  fi
  sleep 2
done

echo "--- container logs ---" >&2
docker logs "$container" >&2 || true
fail "/healthz never answered 200"
