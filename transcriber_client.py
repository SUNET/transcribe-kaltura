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


    def add_task(self, task):

        data = {
            "billing_id": task["billingRef"],
            "model": task["model"],
            "output_format": "srt",
            "user_id": "kaltura-adaptor",
            "file_url": task["file_url"],
            "id": task["billingRef"]
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

        res = requests.post(self.__baseurl + "/transcriber/external", data=json.dumps(data), headers=self.__headers)

        if res.status_code < 200 or res.status_code > 299:
            logger.warning("Error adding new Transcriber task: %s (%i) %s", str(task), res.status_code, res.content)
            return None
        # TODO Add error handling
        # logger.debug(res.json())

        return res.json()

    def get_tasks_by_ref_id(self, refIds):
        job_id=refIds[0]

        res = requests.get(self.__baseurl + "/transcriber/external/{}".format(job_id), headers=self.__headers)
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

    def get_task_result(self, refId, output_format):

        # @router.get("/transcriber/external/{external_id}/result/{output_format}")
        # async def get_transcription_result_external(
        #     request: Request,
        #     external_id: str,
        #     output_format: OutputFormatEnum,
        #     user_id: str = Depends(get_current_user_id),
        # ) -> FileResponse:

        res = requests.get(self.__baseurl + "/transcriber/external/{}/result/{}".format(refId, output_format), headers=self.__headers)
        logger.info("Get_task_result: {}".format(res))
        return res


    def build_task_result_url(self, endpoint):

        return self.__baseurl + endpoint





