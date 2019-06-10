import os
import requests
import logging
import json
import time

from flink_scrat.exception_classes import FailedSavepointException, MaxRetriesReachedException

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

	def cancel_job_w_savepoint(self, job_id, target_dir, max_retries = 20):
		logger.info("Cancelling Job=<{}>".format(job_id))
		route = "{}/jobs/{}/savepoints/".format(self.path, job_id)

		body = json.dumps({
					"target-directory": target_dir,
					"cancel-job": True
				})

		response = self.handle_response(requests.post(route, data=body))
		
		if response is not None:
			logger.info("Triggered savepoint for job=<{}>".format(job_id))
			trigger_id = response["request-id"]

			for try_num in range(0, max_retries):
				trigger_info = self.savepoint_trigger_info(job_id, trigger_id)
				trigger_status = trigger_info['status']['id']

				if trigger_status == 'IN_PROGRESS':
					logger.debug("Savepoint still in progress. Try {}".format(try_num))
					time.sleep(1)
					continue
				else:
					try:
						savepoint_path = trigger_info['operation']['location']
						logger.info("Savepoint completed. Job Cancelled")

					except:
						logger.warning("Savepoint failed.")
						raise FailedSavepointException(trigger_info['operation']['failure-cause']['stack-trace'])

					return savepoint_path

		logger.warning("Savepoint failed. Max retries exceded. Aborting deploy")
		raise MaxRetriesReachedException()
		
		return None

	def run_job(self, jar_id):
		route = "{}/jars/{}/run".format(self.path, jar_id)
		response = self.handle_response(requests.post(route))

		return response

	def run_job_from_savepoint(self, jar_id, savepoint_path):
		logger.info("Restoring job from savepoint=<{}>".format(savepoint_path))
		body = json.dumps({
			'savepointPath': savepoint_path
			})

		route = self.path + "jars/{}/run".format(jar_id, data=body)
		response = self.handle_response(requests.post(route))

		return response

	def savepoint_trigger_info(self, job_id, trigger_id):
		route =  "{}/jobs/{}/savepoints/{}".format(self.path, job_id, trigger_id)

		return self.handle_response(requests.get(route))


	def submit_jar(self, jar_path):
		with open(jar_path, "rb") as jar:
			jar_name = os.path.basename(jar_path)
			file_dict = {'files': (jar_name, jar)}

			route = "{}/jars/upload".format(self.path)
			response = self.handle_response(requests.post(route, files=file_dict))


			jar_id = os.path.basename(response['filename'])

			if response is not None:
				logger.info("Sucessfully uploaded JAR to cluster")
				jar_id = response['filename'].rsplit("/", 1)[1]
			else:
				logger.warning("Unable to upload JAR to cluster")

			return jar_id

	def job_info(self, job_id):
		route = "{}/jobs/{}".format(self.path, job_id)

		return self.handle_response(requests.get(route))

	def submit_job(self, jar_path, target_dir=None, job_id= None):
		logger.info("Submiting job to cluster")

		body = None
		if job_id is not None:
			logger.info("Triggering savepoint for job=<{}>".format(job_id))
			savepoint_path = self.cancel_job_w_savepoint(job_id, target_dir)
			
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

	def cancel_job(self, job_id):
		params = {"mode": "cancel"}
		route = "{}/jobs/{}".format(self.path, job_id)

		try:
			return self.handle_response(requests.patch(route, params= params))
		except:
			logger.warning("Could not find job=<{}>".format(job_id))
			return None


		