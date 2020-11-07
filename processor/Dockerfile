FROM ubuntu:20.04

ENV BUILD_DIR=/build
ENV SERVICE_NAME=processor

WORKDIR ${BUILD_DIR}

RUN apt update
RUN apt install wait-for-it python3-pip -y

COPY requirements.txt .

RUN pip3 install -r requirements.txt

COPY ${SERVICE_NAME}/ .

CMD wait-for-it kafka:9092 && python3 run.py
