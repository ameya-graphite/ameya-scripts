# syntax=docker/dockerfile:1

FROM python:3.10.8-bullseye

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python3", "trigger_ingestion.py"]
