import logging
from api.settings import API_HOST, API_PORT
from api.endpoints import app


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    app.run(host=API_HOST, port=API_PORT)
