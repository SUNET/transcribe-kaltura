import logging
import requests
import json

logger = logging.getLogger(__name__)


class TranscriberClient:

    def __init__(self, hostname, token):
        self.__hostname = hostname
        self.__baseurl = hostname + "/api/v1"
        self.__token = token
        self.__headers = {
            "x-client-dn": "Kaltura-adaptor",
            "Content-Type": "application/json"
        }
        self.__certs = {
            "ca_crt": "/var/opt/certs/ca.crt",
            "client_crt": "/var/post/certs/adapter.crt",
            "client_key": "/var/post/certs/adapter.key"
        }


    def add_task(self, task):

        data = {
            "model": task["model"],
            "output_format": "srt",
            "user_id": "kaltura-adaptor",
            "file_url": task["file_url"],
            "id": task["billingRef"],
            "language": task["language"]
        }

        # newTask = {
        #     "prirority": "Medium",
        #     "model": model,
        #     # "model": "whisper_large_kb_se",
        #     "billingRef": str(task.id)
        # }

        # language = data.get("language")
        # external_id = data.get("id")
        # model = data.get("model")
        # output_format = data.get("output_format")
        # billing_id = data.get("billing_id")
        # user_id = data.get("user_id")

        res = requests.post(
            self.__baseurl + "/transcriber/external",
            data=json.dumps(data),
            headers=self.__headers,
            cert=(self.__certs["client_crt"], self.__certs["client_key"]),  # client certificate + key
            verify=self.__certs["ca_crt"],
        )

        if res.status_code < 200 or res.status_code > 299:
            logger.warning("Error adding new Transcriber task: %s (%i) %s", str(task), res.status_code, res.content)
            return None
        # TODO Add error handling
        # logger.debug(res.json())

        return res.json()

    def get_tasks_by_ref_id(self, refIds):
        job_id=refIds[0]

        res = requests.get(
            self.__baseurl + "/transcriber/external/{}".format(job_id),
            headers=self.__headers,
            cert=(self.__certs["client_crt"], self.__certs["client_key"]),  # client certificate + key
            verify=self.__certs["ca_crt"],
        )
        print(f"Request: {res.request.method} {res.request.url}")
        print("Headers:", res.request.headers)
        print("Body:", res.request.body)
        print("Response code:", res.status_code)

        if res.status_code < 200 or res.status_code > 299:
            logger.warning("Error fetching Transcriber tasks by refIds: %s (%i) %s", str(refIds), res.status_code, res.content)
            return None
        # TODO Add error handling
        # logger.debug(res.json())

        return res.json()





