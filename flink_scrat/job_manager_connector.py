import requests
import time

class FlinkJobmanagerConnector():
	def __init__(self, address, port):
		self.path = "http://{}:{}/".format(address, port)

	def handle_response(self, req_response):
		if(req_response.status_code == 200):
			print("OK")
			return req_response.json()
		else:
			return None

	def list_jars(self):
		route = self.path + "jars"
		response = self.handle_response(requests.get(route))

		return response

	def delete_jar(self, jar_id):
		route = self.path + "jars/{}".format(jar_id)
		response = self.handle_response(requests.delete(route))

		return response

	def submit_jar(self, jar_path):
		with open(jar_path, "rb") as jar:
			fileDict = {'files': jar}
			route = self.path + "jars/upload"
			response = self.handle_response(requests.post(route, files=fileDict))

			jar_id = response['filename'].rsplit("/", 1)[1]

			return jar_id

	def job_info(self, job_id):
		route = self.path + "jobs/{}".format(job_id)

		return handle_response(requests.get(route))


	def submit_job(self, jar_path, job_id = None):
		jar_id = self.submit_jar(jar_path)

		route = self.path + "jars/{}/run".format(jar_id)
		response = self.handle_response(requests.post(route))

		return response

	def list_jobs(self):
		route = self.path + "jobs"
		response = self.handle_response(requests.get(route))

		return response