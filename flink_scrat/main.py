import argparse
import logging
import os
import yaml

from envparse import env
from logging import CRITICAL, ERROR, DEBUG, INFO, WARN
from flink_scrat.job_manager_connector import FlinkJobmanagerConnector

logger = logging.getLogger(__name__)

def setup_logging():
    """ Setup logging configuration """
    default_level = INFO
    log_level = env('SCRAT_LOG_LEVEL', default=None)
    if log_level:
        level_parser = {
            'CRITICAL': CRITICAL,
            'ERROR': ERROR,
            'WARN': WARN,
            'DEBUG': DEBUG,
            'INFO': INFO}
        default_level = level_parser[log_level]

    path = 'logging.yaml'
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = yaml.safe_load(f.read())
            if log_level:
                loggers = config.setdefault('loggers', {})
                delay_ruler_logger = loggers.setdefault('delay_ruler', {})
                delay_ruler_logger['level'] = default_level
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)

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

if __name__ == "__main__":
	main()

