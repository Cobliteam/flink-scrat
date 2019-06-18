import argparse
import logging

from flink_scrat.utils import setup_logging
from flink_scrat.job_manager_connector import FlinkJobmanagerConnector

logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="A python client to deploy Flink applications to a remote cluster")

    parser.add_argument("--address", dest="address", required=False, default='localhost',
                        help="Address for Flink JobManager")

    parser.add_argument("--port", dest="port", required=False, default=8081,
                        help="Port for Flink JobManager (default 8081)")

    cmds = parser.add_subparsers(help="sub-command help")

    submit_parser = cmds.add_parser('submit', help="Submit a job to the flink cluster")

    submit_parser.add_argument("--jar-path", dest="jar_path", required=True,
                               help="Path for jar to be deployed")

    submit_parser.add_argument("--target-dir", dest="target_dir", required=False,
                               help="Target directory to log job savepoints")

    submit_parser.add_argument("--job-id", dest="job_id", required=False,
                               help="Unique identifier for job to be restored")

    submit_parser.set_defaults(action="submit")

    cancel_parser = cmds.add_parser('cancel', help="Cancel a running job")

    cancel_parser.add_argument("--s", dest="savepoint", required=False, action='store_true',
                               help="Trigger a savepoint before canceling a job")

    cancel_parser.add_argument("--target-dir", dest="target_dir", required=False,
                               help="Target directory to log job savepoints")

    cancel_parser.add_argument("--job-id", dest="job_id", required=True,
                               help="Unique identifier for job to be canceled")

    cancel_parser.set_defaults(action="cancel")

    args = parser.parse_args()
    return args


def main():
    setup_logging()
    args = parse_args()

    address = args.address
    port = args.port
    action = args.action

    conn = FlinkJobmanagerConnector(address, port)

    if action == "submit":
        conn.submit_job(args.jar_path, args.target_dir, args.job_id)
    elif action == "cancel":
        if args.savepoint is None:
            conn.cancel_job(args.job_id)
        else:
            conn.cancel_job_with_savepoint(args.job_id, args.target_dir)


if __name__ == "__main__":
    main()
