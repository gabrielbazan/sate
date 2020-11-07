from requests import post
from settings import (
    PROCESSING_REQUESTS_SERVICE_URI,
    PROCESSING_REQUESTS_SERVICE_SUCCESS_CODE,
)


class ProcessingRequestsServiceError(Exception):
    pass


class ProcessingRequestsService(object):

    @staticmethod
    def create(payload):
        response = post(PROCESSING_REQUESTS_SERVICE_URI, json=payload)

        status_code = response.status_code
        response_text = response.text

        if status_code != PROCESSING_REQUESTS_SERVICE_SUCCESS_CODE:
            raise ProcessingRequestsServiceError(
                f"Failed to create processing request. "
                f"Response from processing_requests service: '{response_text}'"
            )

        try:
            return response.json()
        except ValueError:
            raise ProcessingRequestsServiceError(
                f"Failed to parse response. "
                f"Response from processing_requests service: '{response_text}'"
            )
