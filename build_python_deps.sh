#!/usr/bin/env bash
# ------------------------------------------------------------------------------
# build_python_deps.sh
#
# Builds python-deps.zip: the Couchbase Python SDK and its dependencies, as
# pre-built manylinux2014 x86_64 wheels for the Python version that the
# Amazon Managed Service for Apache Flink runtime actually runs.
#
#   Managed Flink runtime 1.19 / 1.20  ->  Python 3.11
#   (Flink 2.x runtimes                ->  Python 3.12)
#
# Nothing is compiled. Nothing depends on the machine you run this on: the
# wheels are downloaded for the TARGET platform/interpreter, unpacked, and
# zipped with every package at the top level of the archive. That is the layout
# PyFlink expects for a zip passed through `pyFiles` (it is appended to the
# Python worker's sys.path as-is).
#
# Usage:
#   ./build_python_deps.sh                # Python 3.11, x86_64 (Managed Flink 1.19/1.20)
#   PY_VER=3.12 ./build_python_deps.sh    # for a Flink 2.x runtime
#   CB_SDK_VER=4.6.3 ./build_python_deps.sh
# ------------------------------------------------------------------------------
set -euo pipefail

PY_VER="${PY_VER:-3.11}"
PLATFORM="${PLATFORM:-manylinux2014_x86_64}"
CB_SDK_VER="${CB_SDK_VER:-4.6.3}"
OUT_ZIP="${OUT_ZIP:-python-deps.zip}"
WORK="$(mktemp -d)"

echo "==> Target: Python ${PY_VER}, platform ${PLATFORM}, couchbase==${CB_SDK_VER}"

# 1. Download binary wheels only, for the target interpreter and platform.
#    --only-binary=:all: guarantees pip never falls back to compiling from source.
python3 -m pip download \
  "couchbase==${CB_SDK_VER}" \
  --only-binary=:all: \
  --platform "${PLATFORM}" \
  --python-version "${PY_VER}" \
  --implementation cp \
  --dest "${WORK}/wheels" \
  --quiet

echo "==> Wheels downloaded:"
ls -1 "${WORK}/wheels"

# 2. Unpack every wheel so that packages sit at the top level of the zip.
mkdir -p "${WORK}/site"
for whl in "${WORK}/wheels"/*.whl; do
  python3 -m zipfile -e "${whl}" "${WORK}/site"
done

# 3. Sanity checks on what we are about to ship.
CORE_SO="$(find "${WORK}/site/couchbase" -name '_core*.so' | head -n1)"
echo "==> Native core module: ${CORE_SO#${WORK}/site/}"
if command -v file >/dev/null 2>&1; then file "${CORE_SO}"; fi

# 4. Zip it. The archive must contain 'couchbase/', 'typing_extensions.py', ...
#    directly at the root - NOT inside a 'site-packages/' or 'python/' folder.
rm -f "${OUT_ZIP}"
( cd "${WORK}/site" && python3 -m zipfile -c "${OLDPWD}/${OUT_ZIP}" . )

# 5. Manifest for the ticket / for support.
{
  echo "built_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "target_python=${PY_VER}"
  echo "target_platform=${PLATFORM}"
  echo "couchbase_sdk=${CB_SDK_VER}"
  echo "wheels:"
  ls -1 "${WORK}/wheels" | sed 's/^/  /'
} > python-deps.manifest.txt

echo "==> Wrote ${OUT_ZIP} ($(du -h "${OUT_ZIP}" | cut -f1)) and python-deps.manifest.txt"
rm -rf "${WORK}"
