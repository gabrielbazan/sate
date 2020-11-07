import logging
from listener import ResultsListenerBuilder


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    results_listener = ResultsListenerBuilder.build()

    results_listener.start()
