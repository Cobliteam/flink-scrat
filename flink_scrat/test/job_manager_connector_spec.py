from flink_scrat.job_manager_connector import FlinkJobmanagerConnector

from unittest import TestCase
from nose.tools import assert_is_none, assert_equal

FLINK_ADDRESS = 'localhost'
FLINK_PORT = 8081

JAR_NAME = "wordcount-assembly-0.1-SNAPSHOT.jar"
JAR_PATH = "/Users/nicolautahan/Cobliteam/flink-scrat/flink_scrat/test/resources/" + JAR_NAME
not_jar_path = "/Users/nicolautahan/Cobliteam/flink-scrat/flink_scrat/test/resources/not_a_jar.txt"


class FlinkJobmanagerConnectorSpec(TestCase):
	def setUp(self):
		self.connector = FlinkJobmanagerConnector(FLINK_ADDRESS, FLINK_PORT)

	def tearDown(self):
		json_list_jars = self.connector.list_jars()
		jar_ids = [file['id'] for file in json_list_jars['files']]

		for jar_id in jar_ids:
			self.connector.delete_jar(jar_id)

	def test_submit_jar(self):
		jar_id = self.connector.submit_jar(JAR_PATH)
		assert_equal(JAR_NAME in jar_id, True)

		jar_id = self.connector.submit_jar(not_jar_path)
		assert_is_none(jar_id)

	def test_list_jars(self):
		expected_jar_id = self.connector.submit_jar(JAR_PATH)

		json_list_jars = self.connector.list_jars()
		jar_ids = [file['id'] for file in json_list_jars['files']]

		assert_equal(expected_jar_id in jar_ids, True)

	def test_delete_jars(self):
		expected_jar_id = self.connector.submit_jar(JAR_PATH)

		self.connector.delete_jar(expected_jar_id)

		json_list_jars = self.connector.list_jars()
		jar_ids = [file['id'] for file in json_list_jars['files']]

		assert_equal(expected_jar_id not in jar_ids, True)

	def test_submit_jobs(self):
		expected_job_status = "RUNNING"

		responseJson = self.connector.submit_job(JAR_PATH)
		job_id = responseJson["jobid"]
		job_info = self.connector.job_info(job_id)
		
		assert_equal(job_info["state"], expected_job_status)

		not_a_jar_response_json = self.connector.submit_job(not_jar_path)
		assert_is_none(not_a_jar_response_json)