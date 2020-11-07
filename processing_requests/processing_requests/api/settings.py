

MONGODB_HOST = "processing_requests_db"
MONGODB_PORT = 27017
MONGODB_USERNAME = "processing_requests_user"
MONGODB_PASSWORD = "processing_requests_password"
MONGODB_DATABASE_NAME = "processing_requests_db"
MONGODB_PROCESSING_REQUESTS_COLLECTION_NAME = "processing_requests"
MONGODB_EXECUTIONS_COLLECTION_NAME = "executions"


KAFKA_BOOTSTRAP_SERVER = "kafka:9092"
KAFKA_VALUE_ENCODING = "utf-8"
KAFKA_TOPIC = "processing_requests_executions"
KAFKA_ACKS = "all"


API_HOST = "0.0.0.0"
API_PORT = 5000


PROCESSING_REQUESTS_ENDPOINT = "/requests"
EXECUTIONS_ENDPOINT = "/requests/<request_id>/executions"
