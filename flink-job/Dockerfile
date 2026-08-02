FROM flink:1.20.0-scala_2.12-java17

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir apache-flink==1.20.0 redis==5.2.1 clickhouse-connect==0.8.18

RUN curl -fsSL https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.3.0-1.20/flink-connector-kafka-3.3.0-1.20.jar \
    -o /opt/flink/lib/flink-connector-kafka-3.3.0-1.20.jar

WORKDIR /opt/flink/usrlib

COPY app ./app
COPY submit-job.sh /submit-job.sh

RUN chmod +x /submit-job.sh

USER flink
