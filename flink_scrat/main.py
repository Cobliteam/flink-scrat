import argparse
import logging

from flink_scrat.utils import setup_logging
from flink_scrat.job_manager_connector import FlinkJobmanagerConnector
from flink_scrat.utils import find_manager_address

logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(
        description="A python client to deploy Flink applications to a remote cluster")

    address_group = parser.add_mutually_exclusive_group()
    port_group = parser.add_mutually_exclusive_group()

    parser.add_argument("--y", dest="use_yarn", required=False, default=False, action='store_true',
                        help="If set, flink-scrat will use the {--app-id; --yarn-address; --yarn-port} \
                        to try finding the correct yarn-session info. Otherwise, it will assume that the correct \
                        JobManager info is provided in {--address; --port}")

    parser.add_argument("--app-id", dest="app_id", required=False, default="",
                        help="Identifier tag for the Flink yarn-session. If not set, it will be assumed that \
                        the yarn-session has no identifier")

    address_group.add_argument("--yarn-address", dest="yarn_address", required=False,
                               help="Address for the yarn manager")

    port_group.add_argument("--yarn-port", dest="yarn_port", required=False,
                            help="Port for the yarn manager")

    address_group.add_argument("--address", dest="address", required=False, default='localhost',
                               help="Address for Flink JobManager")

    port_group.add_argument("--port", dest="port", required=False, default=8081,
                            help="Port for Flink JobManager (default 8081)")

    cmds = parser.add_subparsers(help="sub-command help")

    submit_parser = cmds.add_parser('submit', help="Submit a job to the flink cluster")
    savepoint_group = submit_parser.add_mutually_exclusive_group()

    savepoint_group.add_argument("--savepoint-path", dest="savepoint_path", required=False,
                                 help="Restore a job from a savepoint")

    submit_parser.add_argument("--jar-path", dest="jar_path", required=True,
                               help="Path for jar to be deployed")

    savepoint_group.add_argument("--target-dir", dest="target_dir", required=False,
                                 help="Target directory to log job savepoints")

    submit_parser.add_argument("--job-id", dest="job_id", required=False,
                               help="Unique identifier for job to be restored")

    submit_parser.add_argument("--parallelism", dest="parallelism", required=False,
                               help="Number of parallelism for the job tasks")

    submit_parser.add_argument("--entry-class", dest="entry_class", required=False,
                               help="Main class for runnning the job")

    submit_parser.add_argument("--allow-non-restore", dest="anr", required=False,
                               help="If present allows job to start even if restore from savepoint fails")

    submit_parser.add_argument("--extra-args", dest="extra_args", required=False,
                               help="""Extra comma-separeted configuration options.
                                See https://ci.apache.org/projects/flink/flink-docs-release-1.8/ops/config.html""")

    submit_parser.set_defaults(action="submit")

    cancel_parser = cmds.add_parser('cancel', help="Cancel a running job")

    cancel_parser.add_argument("--s", dest="savepoint", required=False, action='store_true',
                               help="Trigger a savepoint before canceling a job")

    cancel_parser.add_argument("--target-dir", dest="target_dir", required=False,
                               help="Target directory to log job savepoints")

    cancel_parser.add_argument("--job-id", dest="job_id", required=True,
                               help="Unique identifier for job to be canceled")

    cancel_parser.set_defaults(action="cancel")

    savepoint_parser = cmds.add_parser('savepoint', help="Trigger a savepoint for a running Job")

    savepoint_parser.add_argument("--target-dir", dest="target_dir", required=True,
                                  help="Target directory to log job savepoints")

    savepoint_parser.add_argument("--job-id", dest="job_id", required=True,
                                  help="Unique identifier for job to be snapshot")

    savepoint_parser.set_defaults(action="savepoint")

    args = parser.parse_args()

    if args.use_yarn is True and (args.yarn_address is None or args.yarn_port is None):
        parser.error("When setting --y, {--yarn-address; --yarn-port} must be provided")

    return args


def main():
    setup_logging()
    args = parse_args()

    use_yarn = args.use_yarn

    if use_yarn is True:
        yarn_info = find_manager_address(args.yarn_address, args.yarn_port, args.app_id)

        manager_address = yarn_info['rpc_address']
        manager_port = yarn_info['rpc_port']
    else:
        manager_address = args.address
        manager_port = args.port

    conn = FlinkJobmanagerConnector(manager_address, manager_port)

    action = args.action

    if action == "submit":
        conn.submit_job(args.jar_path, args.savepoint_path, args.target_dir, args.job_id, args.anr,
                        args.parallelism, args.entry_class, args.extra_args)
    elif action == "cancel":
        if args.savepoint is None:
            conn.cancel_job(args.job_id)
        else:
            conn.cancel_job_with_savepoint(args.job_id, args.target_dir)
    elif action == "savepoint":
        savepoint_path = conn.trigger_savepoint(args.job_id, args.target_dir)
        logging.info("Savepoint completed at <{}> for Job=<{}>".format(
            savepoint_path, args.job_id))


if __name__ == "__main__":
    main()
