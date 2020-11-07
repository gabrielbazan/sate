from requests import get
from settings import (
    RESULTS_SERVICE_URI,
    RESULTS_SERVICE_SUCCESS_CODE,
    RESULTS_SERVICE_REQUEST_PARAMETER,
    RESULTS_SERVICE_RESULTS_LIST_KEY,
    RESULT_IS_SUCCESS_KEY,
)


class ResultsServiceError(Exception):
    pass


class ResultsService(object):

    @staticmethod
    def get_results(request_id):
        uri = RESULTS_SERVICE_URI.format(
            ** {
                RESULTS_SERVICE_REQUEST_PARAMETER: request_id
            }
        )

        response = get(uri)

        status_code = response.status_code
        response_text = response.text

        if status_code != RESULTS_SERVICE_SUCCESS_CODE:
            raise ResultsServiceError(
                f"Failed to retrieve results. "
                f"Response from results_api service: '{response_text}'"
            )

        try:
            return response.json()[RESULTS_SERVICE_RESULTS_LIST_KEY]
        except ValueError:
            raise ResultsServiceError(
                f"Failed to parse response. "
                f"Response from results_api service: '{response_text}'"
            )

    @staticmethod
    def has_been_successfully_processed(request_id):
        results = ResultsService.get_results(request_id)

        for result in results:
            is_success = result.get(RESULT_IS_SUCCESS_KEY)
            if is_success:
                return result

    @staticmethod
    def get_failures_count(request_id):
        results = ResultsService.get_results(request_id)

        failures_count = 0

        for result in results:
            is_success = result.get(RESULT_IS_SUCCESS_KEY)
            if not is_success:
                failures_count += 1

        return failures_count
