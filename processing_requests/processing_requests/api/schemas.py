

EXECUTIONS_KEY = "executions"


class ProcessingRequestField(object):
    ID = "id"
    PAYLOAD = "payload"


class ExecutionField(object):
    ID = "id"
    PROCESSING_REQUEST = "processing_request"
    FORCE_ERROR = "force_error"
    CREATION_DATE = "creation_date"

    PROCESSING_REQUEST_ID = f"{PROCESSING_REQUEST}.{ProcessingRequestField.ID}"
