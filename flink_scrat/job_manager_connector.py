import os
import requests

from http import HTTPStatus

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

	def submit_jar(self, jar_path):
		with open(jar_path, "rb") as jar:
			jar_name = os.path.basename(jar_path)
			file_dict = {'files': (jar_name, jar)}

			route = "{}/jars/upload".format(self.path)
			response = self.handle_response(requests.post(route, files=file_dict))

			jar_id = os.path.basename(response['filename'])

			return jar_id

	def job_info(self, job_id):
		route = "{}/jobs/{}".format(self.path, job_id)

		return self.handle_response(requests.get(route))


	def submit_job(self, jar_path):
		jar_id = self.submit_jar(jar_path)

		route = "{}/jars/{}/run".format(self.path, jar_id)
		response = self.handle_response(requests.post(route))

		return response

	def list_jobs(self):
		route = "{}/jobs".format(self.path)
		response = self.handle_response(requests.get(route))

		return response
		