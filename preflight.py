"""
preflight.py - Managed Flink / Couchbase SDK runtime diagnostics.

Deploy this as the Python entry point (instead of main.py) with the SAME zip,
the same `pyFiles` and the same VPC configuration as the real job. It runs a
one-record pipeline whose map function executes INSIDE the PyFlink Python
worker - the exact place where `import couchbase` is failing today - and
reports, in plain text, what that interpreter looks like and what happens on
import and on connecting to Capella.

Everything is caught and reported: this job should never fail. If it does,
the failure is in front of the Python worker (zip layout / entry point), not in
the SDK.

Where to read the output
  CloudWatch Logs -> log group of the application -> TaskManager log stream.
  Search for the string "PREFLIGHT".

Runtime properties (optional, group "CouchbaseSink"):
  connection_string   couchbases://cb.xxxx.cloud.couchbase.com
  username            cluster access credential
  password            cluster access credential
  bucket              bucket to open (ping only, nothing is written)
"""
import json
import logging
import os
import platform
import sys
import traceback

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment

log = logging.getLogger("preflight")


def _runtime_properties():
    """Read Managed Flink runtime properties (falls back to env vars locally)."""
    props = {}
    try:
        from pyflink.java_gateway import get_gateway  # noqa: F401
        path = "/etc/flink/application_properties.json"
        if os.path.exists(path):
            with open(path) as f:
                for group in json.load(f):
                    if group.get("PropertyGroupId") == "CouchbaseSink":
                        props.update(group.get("PropertyMap", {}))
    except Exception:  # pragma: no cover - diagnostics only
        pass
    for k in ("connection_string", "username", "password", "bucket"):
        if k not in props and os.environ.get(f"CB_{k.upper()}"):
            props[k] = os.environ[f"CB_{k.upper()}"]
    return props


def diagnose(_):
    """Runs inside the Python worker. Returns one multi-line report string."""
    lines = ["PREFLIGHT report"]
    lines.append(f"python_executable={sys.executable}")
    lines.append(f"python_version={sys.version.split()[0]}")
    lines.append(f"machine={platform.machine()} system={platform.system()}")
    try:
        lines.append(f"glibc={platform.libc_ver()[1]}")
    except Exception:
        pass
    lines.append("sys.path=" + " | ".join(sys.path))

    # 1. Can the SDK package be found and its native core loaded?
    try:
        import couchbase  # noqa: F401
        from couchbase.logic.pycbc_core import _core  # forces the .so to load
        lines.append(f"import_couchbase=OK version={couchbase.__version__}")
        lines.append(f"native_core={_core.__file__}")
    except BaseException:
        lines.append("import_couchbase=FAILED")
        lines.append(traceback.format_exc())
        return "\n".join(lines)

    # 2. Can the worker reach Capella from inside the Managed Flink VPC?
    props = _runtime_properties()
    if not props.get("connection_string"):
        lines.append("capella_connect=SKIPPED (no CouchbaseSink.connection_string property)")
        return "\n".join(lines)
    try:
        from datetime import timedelta
        from couchbase.auth import PasswordAuthenticator
        from couchbase.cluster import Cluster
        from couchbase.options import ClusterOptions, ClusterTimeoutOptions

        opts = ClusterOptions(
            PasswordAuthenticator(props["username"], props["password"]),
            timeout_options=ClusterTimeoutOptions(bootstrap_timeout=timedelta(seconds=20)),
        )
        cluster = Cluster(props["connection_string"], opts)
        cluster.wait_until_ready(timedelta(seconds=20))
        ping = cluster.ping()
        lines.append("capella_connect=OK")
        for svc, endpoints in ping.endpoints.items():
            for ep in endpoints:
                lines.append(f"  ping {svc.name} {ep.remote} state={ep.state.name} latency={ep.latency}")
        if props.get("bucket"):
            cluster.bucket(props["bucket"]).default_collection()
            lines.append(f"bucket_open={props['bucket']} OK")
    except BaseException:
        lines.append("capella_connect=FAILED")
        lines.append(traceback.format_exc())
    return "\n".join(lines)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    # Local testing only: on Managed Flink the zip is supplied through the
    # `pyFiles` runtime property, which does the same thing.
    if os.environ.get("PREFLIGHT_PYFILES"):
        env.add_python_file(os.environ["PREFLIGHT_PYFILES"])
    report = env.from_collection([1], type_info=Types.INT()) \
        .map(diagnose, output_type=Types.STRING())
    # Both channels: Flink stdout (print sink) and the Python worker logger.
    report.map(lambda r: (log.warning(r), print(r, flush=True), r)[2], output_type=Types.STRING()).print()
    env.execute("couchbase-sdk-preflight")


if __name__ == "__main__":
    main()
