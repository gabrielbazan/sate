import json
from datetime import datetime
import logging
from uuid import uuid4
from werkzeug.exceptions import Conflict, NotFound
from flask import Flask, request, jsonify
from pymongo import MongoClient
from bson.json_util import dumps as bson_dumps
from bson.json_util import loads as bson_loads
from kafka import KafkaProducer
from api.settings import (
    PROCESSING_REQUESTS_ENDPOINT,
    EXECUTIONS_ENDPOINT,
    MONGODB_HOST,
    MONGODB_PORT,
    MONGODB_USERNAME,
    MONGODB_PASSWORD,
    MONGODB_DATABASE_NAME,
    MONGODB_PROCESSING_REQUESTS_COLLECTION_NAME,
    MONGODB_EXECUTIONS_COLLECTION_NAME,
    KAFKA_BOOTSTRAP_SERVER,
    KAFKA_VALUE_ENCODING,
    KAFKA_TOPIC,
    KAFKA_ACKS,
)
from api.schemas import (
    ProcessingRequestField,
    ExecutionField,
    ExecutionStatus,
    EXECUTIONS_KEY,
)


LOGGER = logging.getLogger()


app = Flask(__name__)


@app.route("/")
def root():
    return "Processing Requests API"


@app.route(PROCESSING_REQUESTS_ENDPOINT, methods=["POST"])
def create_processing_request():
    processing_request = request.get_json()

    validate_processing_request_creation(processing_request)

    identifier = generate_identifier()
    processing_request[ProcessingRequestField.ID] = identifier

    get_processing_requests_collection().insert_one(processing_request.copy())

    return processing_request, 201


@app.route(EXECUTIONS_ENDPOINT, methods=["POST"])
def create_execution(request_id):
    processing_request = get_processing_request_or_raise(request_id)

    execution = request.get_json()

    validate_execution_creation(execution)

    executions_collection = get_executions_collection()

    # Add some fields to the execution
    identifier = generate_identifier()
    execution[ExecutionField.ID] = identifier
    execution[ExecutionField.PROCESSING_REQUEST] = processing_request
    execution[ExecutionField.CREATION_DATE] = str(datetime.utcnow())

    # Save to mongo (with 'created' status)
    db_execution = execution.copy()
    db_execution[ExecutionField.STATUS] = ExecutionStatus.CREATED
    executions_collection.insert_one(db_execution)

    # Publish to Kafka
    publish_execution_to_kafka(execution)

    # Update status in mongo (to 'published')
    update_query = {ExecutionField.ID: identifier}
    update_values = {"$set": {ExecutionField.STATUS: ExecutionStatus.PUBLISHED}}
    executions_collection.update_one(update_query, update_values)
    execution[ExecutionField.STATUS] = ExecutionStatus.PUBLISHED

    return execution, 201


@app.route(EXECUTIONS_ENDPOINT, methods=["GET"])
def list_processing_request_executions(request_id):
    get_processing_request_or_raise(request_id)

    query = {
        ExecutionField.PROCESSING_REQUEST_ID: request_id
    }
    include_fields = {
        "_id": False
    }
    executions = get_executions_collection().find(query, projection=include_fields)

    serialized_executions = mongo_object_to_json(executions)

    serialized = {
        EXECUTIONS_KEY: serialized_executions
    }

    return jsonify(serialized), 200


@app.errorhandler(Conflict)
def handle_conflict(e):
    return handle_error(e), 409


@app.errorhandler(NotFound)
def handle_not_found(e):
    return handle_error(e), 404


@app.errorhandler(Exception)
def handle_uncaught_error(e):
    return handle_error(e), 500


def handle_error(e):
    message = str(e)
    return {"message": message}


def validate_processing_request_creation(processing_request):
    payload_field = ProcessingRequestField.PAYLOAD

    if payload_field not in processing_request:
        raise Conflict(f"The processing request does not have '{payload_field}'")

    payload = processing_request.get(payload_field)

    if ProcessingRequestField.MESSAGE not in payload:
        raise Conflict(
            f"The processing request does not have '{ProcessingRequestField.MESSAGE}' "
            f"inside of '{payload_field}'"
        )


def get_processing_request_or_raise(processing_request_id):
    processing_request = get_processing_request_or_none(processing_request_id)
    if not processing_request:
        raise NotFound("Processing request not found")
    return processing_request


def get_processing_request_or_none(processing_request_id):
    include_fields = {
        "_id": False
    }

    processing_request = get_processing_requests_collection().find_one(
        {
            ProcessingRequestField.ID: processing_request_id
        },
        projection=include_fields
    )

    if processing_request:
        return mongo_object_to_json(processing_request)


def validate_execution_creation(execution):
    if ExecutionField.FORCE_ERROR not in execution:
        raise Conflict(f"Please specify the {ExecutionField.FORCE_ERROR} property")


def get_processing_requests_collection():
    return get_database()[MONGODB_PROCESSING_REQUESTS_COLLECTION_NAME]


def get_executions_collection():
    return get_database()[MONGODB_EXECUTIONS_COLLECTION_NAME]


def get_database():
    client = MongoClient(MONGODB_HOST, MONGODB_PORT, username=MONGODB_USERNAME, password=MONGODB_PASSWORD)
    return client[MONGODB_DATABASE_NAME]


def publish_execution_to_kafka(execution):
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVER],
        value_serializer=lambda value: json.dumps(value).encode(KAFKA_VALUE_ENCODING),
        acks=KAFKA_ACKS
    )

    producer.send(KAFKA_TOPIC, value=execution)


def generate_identifier():
    return str(uuid4())


def mongo_object_to_json(mongo_object):
    return bson_loads(bson_dumps(mongo_object))
