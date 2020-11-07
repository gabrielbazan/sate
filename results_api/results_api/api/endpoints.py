import logging
from werkzeug.exceptions import Conflict, NotFound
from flask import Flask, jsonify
from pymongo import MongoClient
from bson.json_util import dumps as bson_dumps
from bson.json_util import loads as bson_loads
from api.settings import (
    MONGODB_HOST,
    MONGODB_PORT,
    MONGODB_USERNAME,
    MONGODB_PASSWORD,
    MONGODB_DATABASE_NAME,
    MONGODB_RESULTS_COLLECTION_NAME,
    RESULTS_ENDPOINT,
)
from api.schemas import ResultField, RESULTS_KEY


LOGGER = logging.getLogger()


app = Flask(__name__)


@app.route("/")
def root():
    return "Results API"


@app.route(RESULTS_ENDPOINT, methods=["GET"])
def list_processing_request_executions_results(request_id):
    query = {
        ResultField.PROCESSING_REQUEST_ID: request_id
    }
    include_fields = {
        "_id": False
    }
    results = get_results_collection().find(query, projection=include_fields)

    serialized_results = bson_loads(bson_dumps(results))

    serialized = {
        RESULTS_KEY: serialized_results
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


def get_results_collection():
    return get_database()[MONGODB_RESULTS_COLLECTION_NAME]


def get_database():
    client = MongoClient(MONGODB_HOST, MONGODB_PORT, username=MONGODB_USERNAME, password=MONGODB_PASSWORD)
    return client[MONGODB_DATABASE_NAME]
