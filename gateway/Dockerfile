FROM python:3.8

ENV BUILD_DIR=/build
ENV SERVICE_NAME=gateway

WORKDIR ${BUILD_DIR}

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY ${SERVICE_NAME}/ .

CMD python run.py
