# PyFlink + Couchbase Python SDK on Amazon Managed Service for Apache Flink

Packaging kit and diagnostics for the SM Retail ODS pipeline
(Qlik CDC -> Amazon MSK -> Managed Flink -> Couchbase Capella).

## What is in here

| File | Purpose |
|---|---|
| `build_python_deps.sh` | Builds `python-deps.zip`: Couchbase Python SDK 4.6.3 + dependencies as pre-built **manylinux2014 x86_64** wheels for **Python 3.11** (the interpreter Managed Flink 1.19/1.20 runs). Nothing is compiled. |
| `preflight.py` | One-record PyFlink job that runs inside the Python worker and reports interpreter version, `sys.path`, whether `import couchbase` loads the native core, and whether Capella is reachable from the Managed Flink VPC. Deploy it with the same zip and VPC settings as the real job; read the report in CloudWatch (search for `PREFLIGHT`). |
| `main.py` | Reference job: MSK topic -> transform -> key-value upsert into a Capella collection. One SDK `Cluster` per worker, created in `open()`. |
| `pom.xml` | Builds `pyflink-dependencies.jar` (Flink Kafka connector 3.4.0-1.20 + MSK IAM auth). |
| `application_properties.json` | Runtime property groups (also used for local runs). |
| `requirements.txt` | Pinned SDK version. |

## Build and deploy

```bash
# 1. Python dependencies (any Linux/macOS machine with Python 3 + pip; no Docker needed)
./build_python_deps.sh                 # -> python-deps.zip, python-deps.manifest.txt

# 2. Java connectors
mvn clean package                      # -> target/pyflink-dependencies.jar

# 3. Application zip (layout matters: packages must sit at the ROOT of python-deps.zip)
mkdir -p app/lib
cp main.py preflight.py app/
cp python-deps.zip target/pyflink-dependencies.jar app/lib/
zip -r smretail-ods-flink.zip app

# 4. Upload to S3, point the Managed Flink application at it, and set runtime properties:
#    kinesis.analytics.flink.run.options: python=app/preflight.py (first), jarfile=app/lib/pyflink-dependencies.jar,
#                                         pyFiles=app/lib/python-deps.zip
#    CouchbaseSink / KafkaSource: see application_properties.json
# 5. Run preflight.py once, read the PREFLIGHT report in CloudWatch, then switch python= to app/main.py.
```

Managed Flink does not run `pip install`; `pyRequirements`, `pyModule` and `pyExecutable`
are not supported. Everything the job imports must be inside the zip and referenced through
`pyFiles` (or `pyArchives`).

## Symptom -> cause table (all reproduced while building this kit)

| What the TaskManager log shows | Cause | Fix |
|---|---|---|
| `ModuleNotFoundError: No module named 'couchbase'` | Packages nested under a folder inside the zip (`site-packages/couchbase/...`, `python/couchbase/...`), or `pyFiles` points at the wrong path | Packages at the zip root; `build_python_deps.sh` does this |
| `ModuleNotFoundError: No module named 'typing_extensions'` | Only the `couchbase` folder was copied; the SDK's pure-Python dependency was left out | Ship every wheel `pip download` returns (the script does) |
| `ImportError: ... _core.so: cannot open shared object file` or `wrong ELF class` | Wheel built for another CPU (`aarch64` on an x86_64 runtime, or the reverse) | Download with `--platform manylinux2014_x86_64` |
| `GLIBC_2.xx not found` | SDK compiled from source inside a newer Debian/Ubuntu image | Do not compile; use the official manylinux2014 wheels (they need glibc >= 2.17 only) |
| `Python process exits with code: 1` with no Python traceback nearby | The traceback is in the TaskManager stream, not the JobManager/exception tab | Filter CloudWatch on `Traceback` or run `preflight.py` |

## What was verified (23 Sep 2026, cloud sandbox, Python 3.11.15, Flink 1.20.5, Java 21)

* Official wheel `couchbase-4.6.3-cp311-cp311-manylinux2014_x86_64` links only against
  `libc`, `libstdc++`, `libm`, `libz`, `libpthread`, `libgcc_s` (`ldd`). TLS is BoringSSL, statically
  linked, so no OpenSSL is needed from the OS. Highest symbol versions: `GLIBC_2.16`, `GLIBCXX_3.4.19`.
* `preflight.py` ran inside a real PyFlink Python worker, in both `process` mode and `thread`
  (pemja, in-JVM) mode, with the SDK delivered **only** through `pyFiles`; `import couchbase`
  loaded the native core and a `Cluster` object was created (connection then timed out against
  a placeholder hostname, as expected with no Capella in reach).
* `main.py` compiled and its `CouchbaseUpsert` function was exercised against a stub collection
  (upsert key, document shape, `MAJORITY` durability option). It has not been run against a live
  MSK topic or Capella cluster yet; that is the R&D validation step.
* PyFlink's own dependency set ships compiled C extensions (pemja, pyarrow, numpy, pandas,
  apache-beam, grpc, fastavro), so a Managed Flink runtime that "blocks C extensions" would not
  be able to start any Python job at all.
