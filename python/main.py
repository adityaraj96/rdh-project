import sys
import logging
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Types

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

def test_couchbase_import(_):
    import platform
    report = f"\n=== PREFLIGHT REPORT ===\nInterpreter: {sys.executable}\nPlatform: {platform.platform()}\nPath: {sys.path}\n"
    
    try:
        from couchbase.cluster import Cluster
        from couchbase.options import ClusterOptions
        from couchbase.auth import PasswordAuthenticator
        
        # INSERT ACTUAL CAPELLA CREDENTIALS HERE
        auth = PasswordAuthenticator("aws_flnk_notebook", "Password@202610")
        cluster = Cluster("couchbases://cb.q8i2gha75y5jqvq6.cloud.couchbase.com", ClusterOptions(auth))
        
        ping_result = cluster.ping()
        report += f"SUCCESS: Python SDK imported natively and Capella pinged.\nEndpoints: {ping_result.endpoints}"
    except ImportError as ie:
        report += f"FATAL IMPORT ERROR: {str(ie)}\n"
    except Exception as e:
        report += f"NETWORK TIMEOUT / ERROR: {str(e)}\n"
        
    logger.info(report)
    yield report

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    trigger = env.from_collection([1], type_info=Types.INT())
    
    check = trigger.flat_map(test_couchbase_import, output_type=Types.STRING()).name("preflight-check")
    check.print()
    
    env.execute("rdh-preflight")

if __name__ == "__main__":
    main()
