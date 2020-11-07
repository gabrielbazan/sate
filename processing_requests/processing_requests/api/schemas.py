

EXECUTIONS_KEY = "executions"


class ProcessingRequestField(object):
    ID = "id"
    PAYLOAD = "payload"
    MESSAGE = "message"


class ExecutionField(object):
    ID = "id"
    PROCESSING_REQUEST = "processing_request"
    FORCE_ERROR = "force_error"
    STATUS = "status"
    CREATION_DATE = "creation_date"

    PROCESSING_REQUEST_ID = f"{PROCESSING_REQUEST}.{ProcessingRequestField.ID}"


class ExecutionStatus(object):
    CREATED = "CREATED"
    PUBLISHED = "PUBLISHED"
