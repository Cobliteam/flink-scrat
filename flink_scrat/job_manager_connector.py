import os
import requests
import logging
import time

from requests.exceptions import HTTPError
from flink_scrat.exceptions import (FailedSavepointException, MaxRetriesReachedException,
                                    NotValidJARException, JobRunFailedException, JobIdNotFoundException)

logger = logging.getLogger(__name__)


class FlinkJobmanagerConnector():

    def __init__(self, address, port):
        self.path = "http://{}:{}".format(address, port)

    def handle_response(self, req_response):
        req_response.raise_for_status()
        return req_response.json()

    def list_jars(self):
        route = "{}/jars".format(self.path)
        response = self.handle_response(requests.get(route))

        return response

    def delete_jar(self, jar_id):
        route = "{}/jars/{}".format(self.path, jar_id)
        response = self.handle_response(requests.delete(route))

        return response

    def _await_savepoint_completion(self, job_id, request_id, max_retries=20, retry_sleep_seconds=2):
        in_progess_status = 'IN_PROGRESS'
        for try_num in range(0, max_retries):
            trigger_info = self.savepoint_trigger_info(job_id, request_id)
            trigger_status = trigger_info['status']['id']

            if trigger_status == in_progess_status:
                logger.debug(
                    "Savepoint still in progress. Try {}".format(try_num))

                time.sleep(retry_sleep_seconds)
                continue
            else:
                savepoint_result = trigger_info['operation']
                if('failure-cause' in savepoint_result.keys()):
                    logger.warning("Savepoint failed.")
                    raise FailedSavepointException(
                        trigger_info['operation']['failure-cause']['stack-trace'])

                else:
                    savepoint_path = savepoint_result['location']
                    logger.info("Savepoint completed path=<{}>. Job Cancelled".format(savepoint_path))

                    return savepoint_path

        logger.warning("Savepoint failed. Max retries exceded.")
        raise MaxRetriesReachedException(
            "Savepoint was not completed in time. Max retries=<{}> reached".format(max_retries))

    def cancel_job_with_savepoint(self, job_id, target_dir):
        logger.info("Cancelling Job=<{}> and adding savepoit to savepoint_path=<{}>".format(job_id, target_dir))
        route = "{}/jobs/{}/savepoints/".format(self.path, job_id)

        body = {
            "target-directory": target_dir,
            "cancel-job": True
        }

        try:
            response = self.handle_response(requests.post(route, json=body))

            if response is not None:
                request_id = response["request-id"]
                logger.info("Triggered savepoint for job=<{}>. Savepoint_request_id=<{}>".format(job_id, request_id))

                return self._await_savepoint_completion(job_id, request_id)
        except HTTPError as e:
            raise JobIdNotFoundException("Could not find JobId=<{}>. Reason=<{}>".format(job_id, e.response.text))

    def run_job(self, jar_id, body=None):
        logger.info("Starting job for deployed JAR=<{}>".format(jar_id))
        route = "{}/jars/{}/run".format(self.path, jar_id)
        try:
            response = self.handle_response(
                requests.post(route, json=body))
            return response
        except HTTPError as e:
            raise JobRunFailedException("Unable to start running job from jar=<{}>. Reason=<{}>"
                                        .format(jar_id, e.response.text))

    def run_job_from_savepoint(self, jar_id, savepoint_path):
        logger.info("Restoring job from savepoint=<{}>".format(savepoint_path))
        body = {
            'savepointPath': savepoint_path
        }

        response = self.run_job(jar_id, body)

        return response

    def savepoint_trigger_info(self, job_id, request_id):
        route = "{}/jobs/{}/savepoints/{}".format(
            self.path, job_id, request_id)

        return self.handle_response(requests.get(route))

    def submit_jar(self, jar_path):
        with open(jar_path, "rb") as jar:
            jar_name = os.path.basename(jar_path)
            file_dict = {'files': (jar_name, jar)}

            route = "{}/jars/upload".format(self.path)
            try:
                response = self.handle_response(
                    requests.post(route, files=file_dict))

                jar_id = os.path.basename(response['filename'])
                logger.info("Sucessfully uploaded JAR=<{}> to cluster".format(jar_id))
                return jar_id
            except HTTPError:
                logger.warning("Unable to upload JAR=<{}> to cluster".format(jar_path))
                raise NotValidJARException("File at {} is not a valid JAR".format(jar_path))

    def job_info(self, job_id):
        route = "{}/jobs/{}".format(self.path, job_id)

        return self.handle_response(requests.get(route))

    def submit_job(self, jar_path, target_dir=None, job_id=None):
        job_params = {
            "jar-path": jar_path,
            "target-directory": target_dir,
            "job-id": job_id
        }

        logger.info("Submiting job to cluster")
        logging.info("Job Parameters=<>{}".format(job_params))
        if job_id is not None and target_dir is not None:
            logger.info("Triggering savepoint for job=<{}>".format(job_id))
            savepoint_path = self.cancel_job_with_savepoint(job_id, target_dir)

            if savepoint_path is not None:
                jar_id = self.submit_jar(jar_path)
                return self.run_job_from_savepoint(jar_id, savepoint_path)

        else:
            jar_id = self.submit_jar(jar_path)
            return self.run_job(jar_id)

    def list_jobs(self):
        route = "{}/jobs".format(self.path)
        response = self.handle_response(requests.get(route))

        return response

    def _is_job_running(self, job_id):
        running_job_status = "RUNNING"
        job_info = self.job_info(job_id)

        return job_info["state"] == running_job_status

    def _await_job_termination(self, job_id, max_retries=20, retry_sleep_seconds=2):
        for try_num in range(0, max_retries):
            if self._is_job_running(job_id):
                logger.debug(
                    "Job is running. Try {}".format(try_num))
                time.sleep(retry_sleep_seconds)
            else:
                logging.info("Job canceled sucessfully")
                return True

        logger.warning("Cancel failed. Max retries exceded.")
        raise MaxRetriesReachedException(
            "Job=<{}> could not be canceled in time. Max retries=<{}> reached".format(job_id, max_retries))

    def cancel_job(self, job_id):
        logging.info("Cancelling Job=<{}>".format(job_id))

        params = {"mode": "cancel"}
        route = "{}/jobs/{}".format(self.path, job_id)

        try:
            self.handle_response(requests.patch(route, params=params))
            return self._await_job_termination(job_id)
        except HTTPError as e:
            raise JobIdNotFoundException("Could not find job=<{}>. Reason=<{}>".format(job_id, e.response.text))
