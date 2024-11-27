# telegraf/Dockerfile

FROM telegraf:latest

# Install dockerize
ENV DOCKERIZE_VERSION v0.6.1

RUN apt-get update && \
    apt-get install -y wget && \
    wget https://github.com/jwilder/dockerize/releases/download/${DOCKERIZE_VERSION}/dockerize-linux-amd64-${DOCKERIZE_VERSION}.tar.gz -O /tmp/dockerize.tar.gz && \
    tar -C /usr/local/bin -xzvf /tmp/dockerize.tar.gz && \
    rm /tmp/dockerize.tar.gz && \
    apt-get remove -y wget && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

# Copy Telegraf configuration
COPY telegraf.conf /etc/telegraf/telegraf.conf

# Entry point to wait for Kafka and InfluxDB before starting Telegraf
ENTRYPOINT ["dockerize", "-wait", "tcp://kafka:9092", "-wait", "tcp://influxdb:8086", "-timeout", "60s", "telegraf"]
