import logging
from requests_processor import RequestsProcessorBuilder


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    requests_processor = RequestsProcessorBuilder.build()

    requests_processor.start()
