import json
import logging
from kafka import KafkaConsumer
from pymongo import MongoClient
from settings import (
    KAFKA_BOOTSTRAP_SERVER,
    KAFKA_VALUE_ENCODING,
    KAFKA_SUCCESSES_TOPIC,
    KAFKA_FAILURES_TOPIC,
    KAFKA_GROUP_ID,
    KAFKA_AUTO_OFFSET_RESET,
    KAFKA_TIMEOUT,
    KAFKA_MAX_RECORDS,
    MONGODB_HOST,
    MONGODB_PORT,
    MONGODB_USERNAME,
    MONGODB_PASSWORD,
    MONGODB_DATABASE_NAME,
    MONGODB_RESULTS_COLLECTION_NAME,
    RESULT_IS_SUCCESS_KEY,
)


IS_SUCCESS_MAP = {
    KAFKA_SUCCESSES_TOPIC: True,
    KAFKA_FAILURES_TOPIC: False,
    # DLQ?
}


LOGGER = logging.getLogger(__name__)


class ResultsListenerBuilder(object):

    @staticmethod
    def build():
        topics = list(IS_SUCCESS_MAP.keys())
        consumer = ResultsListenerBuilder.build_consumer(topics)
        return ResultsListener(consumer)

    @staticmethod
    def build_consumer(topics):
        return KafkaConsumer(
            * topics,
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVER],
            auto_offset_reset=KAFKA_AUTO_OFFSET_RESET,
            enable_auto_commit=False,
            group_id=KAFKA_GROUP_ID,
            value_deserializer=lambda value: json.loads(value.decode(KAFKA_VALUE_ENCODING))
        )


class ResultsListener(object):

    def __init__(self, consumer):
        self.consumer = consumer

    def start(self):
        while True:
            messages_by_partition = self.consumer.poll(
                timeout_ms=KAFKA_TIMEOUT,
                max_records=KAFKA_MAX_RECORDS,
            )
            self.handle_messages(messages_by_partition)

    def handle_messages(self, messages_by_partition):
        for topic_partition, messages in messages_by_partition.items():
            for message in messages:
                self.handle_message(topic_partition, message)

    def handle_message(self, topic_partition, message):
        LOGGER.info("Handling message from: '%s'. Message: '%s'", str(topic_partition), str(message))

        topic_name = topic_partition.topic
        result = message.value.copy()

        is_success = IS_SUCCESS_MAP.get(topic_name)
        result[RESULT_IS_SUCCESS_KEY] = is_success

        LOGGER.info("Result is success: %s", is_success)

        ResultsListener.save_result(result)

        self.commit_current_message(topic_partition, topic_name)

        LOGGER.info("Done handling message from: '%s'. Message: '%s'", str(topic_partition), str(message))

    @staticmethod
    def save_result(result):
        collection = ResultsListener.get_results_collection()
        collection.insert_one(result)

    @staticmethod
    def get_results_collection():
        return ResultsListener.get_database()[MONGODB_RESULTS_COLLECTION_NAME]

    @staticmethod
    def get_database():
        client = MongoClient(MONGODB_HOST, MONGODB_PORT, username=MONGODB_USERNAME, password=MONGODB_PASSWORD)
        return client[MONGODB_DATABASE_NAME]

    def commit_current_message(self, topic_partition, topic_name):
        LOGGER.info("Committing")
        self.consumer.commit()
        new_offset = self.consumer.committed(topic_partition)
        LOGGER.info("Committed. New Kafka offset on '%s' topic: %s", topic_name, new_offset)
