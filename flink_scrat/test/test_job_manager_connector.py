import os
import tempfile
import time
import randompy

from flink_scrat.job_manager_connector import FlinkJobmanagerConnector
from flink_scrat.exceptions import NotValidJARException, JobIdNotFoundException, MaxRetriesReachedException
from nose.tools import assert_equal, assert_is_none, assert_raises, assert_is_not_none
from unittest import TestCase

FLINK_ADDRESS = 'localhost'
FLINK_PORT = 8081

TEST_DIR = os.path.dirname(os.path.abspath(__file__))

JAR_NAME = "wordcount.jar"
JAR_PATH = os.path.join(TEST_DIR, "resources/" + JAR_NAME)
NOT_A_JAR = tempfile.NamedTemporaryFile()


def _await_is_job_running(connector, job_id, max_retries=20, retry_sleep_seconds=2):
    for try_num in range(0, max_retries):
        time.sleep(retry_sleep_seconds)
        if not connector._is_job_running(job_id):
            continue
        else:
            return True

    raise MaxRetriesReachedException()


def submit_job_and_await_running(connector, jar_path):
    response_json = connector.submit_job(jar_path)
    job_id = response_json["jobid"]
    _await_is_job_running(connector, job_id)

    return job_id


class FlinkJobmanagerConnectorSpec(TestCase):
    def setUp(self):
        self.connector = FlinkJobmanagerConnector(FLINK_ADDRESS, FLINK_PORT)
        self.savepoint_dir = "/tmp/savepoints"

    def tearDown(self):
        test_jobs = self.connector.list_jobs()
        for job in test_jobs['jobs']:
            if job['status'] == "RUNNING":
                self.connector.cancel_job(job['id'])

        json_list_jars = self.connector.list_jars()
        jar_ids = [file['id'] for file in json_list_jars['files']]

        for jar_id in jar_ids:
            self.connector.delete_jar(jar_id)

    def test_submit_jar(self):
        jar_id = self.connector.submit_jar(JAR_PATH)
        assert_equal(JAR_NAME in jar_id, True)

    def test_submit_jar_with_not_valid_jar(self):
        with assert_raises(NotValidJARException):
            jar_id = self.connector.submit_jar(NOT_A_JAR.name)

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
        response_json = self.connector.submit_job(JAR_PATH)
        job_id = response_json["jobid"]

        assert_equal(self.connector._is_job_running(job_id), True)

    def test_submit_job_with_not_valid_jar(self):
        with assert_raises(NotValidJARException):
            not_a_jar_response_json = self.connector.submit_job(NOT_A_JAR.name)
            assert_is_none(not_a_jar_response_json)

    def test_cancel_job(self):
        job_id = submit_job_and_await_running(self.connector, JAR_PATH)

        self.connector.cancel_job(job_id)
        assert_equal(self.connector._is_job_running(job_id), False)

    def test_trigger_savepoint(self):
        job_id = submit_job_and_await_running(self.connector, JAR_PATH)

        savepoint_path = self.connector.trigger_savepoint(job_id, self.savepoint_dir)
        assert_is_not_none(savepoint_path)
        assert_equal(self.savepoint_dir in savepoint_path, True)

    def test_cancel_job_with_savepoint(self):
        job_id = submit_job_and_await_running(self.connector, JAR_PATH)

        savepoint_path = self.connector.cancel_job_with_savepoint(job_id, self.savepoint_dir)
        assert_is_not_none(savepoint_path)
        assert_equal(self.savepoint_dir in savepoint_path, True)
        assert_equal(self.connector._await_job_termination(job_id), True)

    def test_cancel_job_with_invalid_job_id(self):
        job_id = randompy.string(10)
        with assert_raises(JobIdNotFoundException):
            self.connector.cancel_job(job_id)

    def test_cancel_job_with_savepoint_invalid_job_id(self):
        job_id = randompy.string(10)
        with assert_raises(JobIdNotFoundException):
            savepoint_path = self.connector.cancel_job_with_savepoint(job_id, self.savepoint_dir)
            assert_is_none(savepoint_path)
