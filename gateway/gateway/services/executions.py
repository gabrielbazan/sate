from requests import post, get
from settings import (
    EXECUTIONS_SERVICE_URI,
    EXECUTIONS_SERVICE_CREATION_SUCCESS_CODE,
    EXECUTIONS_SERVICE_QUERY_SUCCESS_CODE,
    EXECUTIONS_SERVICE_REQUEST_PARAMETER,
    EXECUTIONS_SERVICE_QUERY_LIST_KEY,
    EXECUTION_CREATION_DATE,
    DATETIME_FORMAT,
)


class ExecutionsServiceError(Exception):
    pass


class ExecutionsService(object):
    @staticmethod
    def build_uri(request_id):
        return EXECUTIONS_SERVICE_URI.format(
            ** {
                EXECUTIONS_SERVICE_REQUEST_PARAMETER: request_id
            }
        )

    @staticmethod
    def parse_response(response):
        try:
            return response.json()
        except ValueError:
            raise ExecutionsServiceError(
                "Failed to parse response. "
                f"Response from processing_requests service: '{response.text}'"
            )

    @staticmethod
    def create(request_id, payload):
        uri = ExecutionsService.build_uri(request_id)

        response = post(uri, json=payload)

        status_code = response.status_code
        response_text = response.text

        if status_code != EXECUTIONS_SERVICE_CREATION_SUCCESS_CODE:
            raise ExecutionsServiceError(
                "Failed to create execution. "
                f"Response from processing_requests service: '{response_text}'"
            )

        return ExecutionsService.parse_response(response)

    @staticmethod
    def get_executions(request_id):
        uri = ExecutionsService.build_uri(request_id)

        response = get(uri)

        status_code = response.status_code
        response_text = response.text

        if status_code != EXECUTIONS_SERVICE_QUERY_SUCCESS_CODE:
            raise ExecutionsServiceError(
                "Failed to query executions. "
                f"Response from processing_requests service: '{response_text}'"
            )

        response = ExecutionsService.parse_response(response)

        return response[EXECUTIONS_SERVICE_QUERY_LIST_KEY]

    @staticmethod
    def get_last_execution(request_id):
        executions = ExecutionsService.get_executions(request_id)

        last_execution_timestamp = None
        last_execution = None

        for execution in executions:
            str_timestamp = execution.get(EXECUTION_CREATION_DATE)

            from datetime import datetime

            timestamp = datetime.strptime(str_timestamp, DATETIME_FORMAT)

            if not last_execution_timestamp or last_execution_timestamp < timestamp:
                last_execution_timestamp = timestamp
                last_execution = execution

        return last_execution
