import argparse

from flink_scrat.job_manager_connector import FlinkJobmanagerConnector


def parse_args():
	parser = argparse.ArgumentParser(description="Python client to deploy JVM applications to a remote flink cluster")

	parser.add_argument("--address", dest="address", required=True,
		help="Address for Flink JobManager")

	parser.add_argument("--port", dest="port", required=True,
		help="Port for Flink JobManager default 8081")

	parser.add_argument("--action", dest="action", required=True,
		help="Blah for now")

	parser.add_argument("--jar-path", dest="jar_path", required=False,
		help="Blah for now")

	parser.add_argument("--job-id", dest="job_id", required=False,
		help="Blah for now")

	args = parser.parse_args()
	return (args.address, args.port, args.action, args.jar_path, args.job_id)

def main():
	address, port, action, jar_path, job_id = parse_args()

	conn = FlinkJobmanagerConnector(address, port)

	if action == "submit":
		conn.submit_job(jar_path, job_id)

if __name__ == "__main__":
	main()
