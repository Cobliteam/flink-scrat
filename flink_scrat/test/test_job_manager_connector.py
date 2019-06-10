import os
import tempfile
import time

from flink_scrat.job_manager_connector import FlinkJobmanagerConnector
from flink_scrat.exception_classes import FailedSavepointException, MaxRetriesReachedException
from nose.tools import eq_, assert_is_none, assert_raises, assert_is_not_none
from requests.exceptions import HTTPError
from unittest import TestCase

FLINK_ADDRESS = 'localhost'
FLINK_PORT = 8081

TEST_DIR = os.path.dirname(os.path.abspath(__file__))

JAR_NAME = "wordcount-assembly-0.1-SNAPSHOT.jar"
JAR_PATH = os.path.join(TEST_DIR, "resources/" + JAR_NAME)
NOT_A_JAR = tempfile.NamedTemporaryFile()

def is_job_running(connector, job_id):
	running_job_status = "RUNNING"
	job_info = connector.job_info(job_id)
	
	return job_info["state"] == running_job_status


class FlinkJobmanagerConnectorSpec(TestCase):
	def setUp(self):
		self.connector = FlinkJobmanagerConnector(FLINK_ADDRESS, FLINK_PORT)

	def tearDown(self):
		json_list_jobs = self.connector.list_jobs()
		for job in json_list_jobs['jobs']:
			if job['status'] == "RUNNING":
				self.connector.cancel_job(job['id'])

		json_list_jars = self.connector.list_jars()
		jar_ids = [file['id'] for file in json_list_jars['files']]
	
		for jar_id in jar_ids:
			self.connector.delete_jar(jar_id)

	def test_submit_jar(self):
		jar_id = self.connector.submit_jar(JAR_PATH)
		eq_(JAR_NAME in jar_id, True)

		with assert_raises(HTTPError):
			jar_id = self.connector.submit_jar(NOT_A_JAR.name)

			assert_is_none(jar_id)

	def test_list_jars(self):
		expected_jar_id = self.connector.submit_jar(JAR_PATH)

		json_list_jars = self.connector.list_jars()
		jar_ids = [file['id'] for file in json_list_jars['files']]

		eq_(expected_jar_id in jar_ids, True)

	def test_delete_jars(self):
		expected_jar_id = self.connector.submit_jar(JAR_PATH)

		self.connector.delete_jar(expected_jar_id)

		json_list_jars = self.connector.list_jars()
		jar_ids = [file['id'] for file in json_list_jars['files']]

		eq_(expected_jar_id not in jar_ids, True)

	def test_submit_jobs(self):
		response_json = self.connector.submit_job(JAR_PATH)
		job_id = response_json["jobid"]
		
		eq_(is_job_running(self.connector, job_id), True)

		with assert_raises(HTTPError):
			not_a_jar_response_json = self.connector.submit_job(NOT_A_JAR.name)
			assert_is_none(not_a_jar_response_json)

	def test_cancel_job_w_savepoint(self):
		response_json = self.connector.submit_job(JAR_PATH)
		job_id = response_json["jobid"]
		target_dir = '/tmp/savepoint'
		time.sleep(5)

		savepoint_trigger = self.connector.cancel_job_w_savepoint(job_id, target_dir)
		assert_is_not_none(savepoint_trigger)

		job_id = "Nada"
		with assert_raises(HTTPError):
			savepoint_trigger = self.connector.cancel_job_w_savepoint(job_id, target_dir)
			assert_is_none(savepoint_trigger)


