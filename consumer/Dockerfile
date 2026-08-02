FROM python:3.11-slim

WORKDIR /consumer
COPY consumer.py /consumer

RUN pip install kafka-python minio

CMD ["python", "consumer.py"]
