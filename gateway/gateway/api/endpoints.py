import logging
import json
from werkzeug.exceptions import NotFound, Conflict
from flask import Flask, request
from kafka import KafkaProducer
from settings import (
    PROCESSING_REQUESTS_ENDPOINT,
    EXECUTIONS_ENDPOINT,
    RESULTS_ENDPOINT,
    ALLOWED_FAILURES_COUNT,
    RESULTS_LIST_KEY,
    KAFKA_BOOTSTRAP_SERVER,
    KAFKA_VALUE_ENCODING,
    KAFKA_DEAD_LETTER_QUEUE_TOPIC,
    KAFKA_DEAD_LETTER_QUEUE_ACKS,
)
from services.processing_requests import ProcessingRequestsService
from services.executions import ExecutionsService
from services.results import ResultsService


LOGGER = logging.getLogger()


app = Flask(__name__)


@app.route("/")
def root():
    return "Gateway"


@app.route(PROCESSING_REQUESTS_ENDPOINT, methods=["POST"])
def create_processing_request():
    response = ProcessingRequestsService.create(request.get_json())
    return response, 201


@app.route(EXECUTIONS_ENDPOINT, methods=["POST"])
def create_execution(request_id):
    validate_not_being_processed(request_id)
    validate_does_not_have_successful_result(request_id)

    failures_count = ResultsService.get_failures_count(request_id)
    failed_too_many_times = failures_count >= ALLOWED_FAILURES_COUNT

    if failed_too_many_times:
        last_execution = ExecutionsService.get_last_execution(request_id)
        send_last_execution_to_dead_letter_queue_topic(last_execution)
        raise_failed_too_many_times_error(failures_count)

    response = ExecutionsService.create(request_id, request.get_json())
    return response, 201


@app.route(RESULTS_ENDPOINT, methods=["GET"])
def get_processing_request_results(request_id):
    results = ResultsService.get_results(request_id)
    response = {RESULTS_LIST_KEY: results}
    return response, 200


@app.errorhandler(Conflict)
def handle_conflict(e):
    return handle_error(e), 409


@app.errorhandler(NotFound)
def handle_not_found(e):
    return handle_error(e), 404


# @app.errorhandler(Exception)
def handle_uncaught_error(e):
    return handle_error(e), 500


def handle_error(e):
    message = str(e)
    return {"message": message}


def validate_does_not_have_successful_result(request_id):
    successfully_processed = ResultsService.has_been_successfully_processed(request_id)

    if successfully_processed:
        raise Conflict(
            "The processing request with ID has already been successfully processed. "
            f"Please query its results through the '{RESULTS_ENDPOINT}' endpoint."
        )


def raise_failed_too_many_times_error(failures_count):
    raise Conflict(
        "The processing request failed to execute too many times. "
        f"If failed {failures_count} times, and the maximum allowed "
        f"is {ALLOWED_FAILURES_COUNT}. The last execution has been "
        "sent to a DLQ."
    )


# This should consider cases where something happened internally, and
# maybe it should allow reprocessing if X time passed since the last execution
def validate_not_being_processed(request_id):
    executions = ExecutionsService.get_executions(request_id)
    results = ResultsService.get_results(request_id)

    executions_count = len(executions)
    results_count = len(results)

    if executions_count != results_count:
        raise Conflict(
            "The processing request is being processed at the moment"
        )


def send_last_execution_to_dead_letter_queue_topic(execution):
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVER],
        value_serializer=lambda value: json.dumps(value).encode(KAFKA_VALUE_ENCODING),
        acks=KAFKA_DEAD_LETTER_QUEUE_ACKS
    )

    LOGGER.info("Sending execution to DLQ: '%s'", execution)
    producer.send(KAFKA_DEAD_LETTER_QUEUE_TOPIC, value=execution)
    LOGGER.info("Sent execution to DLQ")
