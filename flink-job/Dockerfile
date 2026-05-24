FROM flink:1.18.0-scala_2.12-java11
RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-dev && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*
RUN pip3 install --no-cache-dir --upgrade pip
COPY requirements.txt /opt/flink/
RUN pip3 install --no-cache-dir -r /opt/flink/requirements.txt
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.0.2-1.18/flink-connector-kafka-3.0.2-1.18.jar && \
    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.0.2/kafka-clients-3.0.2.jar
COPY aggregate.py /opt/flink/
COPY run.sh /opt/flink/
RUN chmod +x /opt/flink/run.sh