# pyspark/Dockerfile

FROM bitnami/spark:3.4.0

WORKDIR /app

# Copy the PySpark application and supporting Python files
COPY pyspark_streaming.py /app/
COPY pipelines/sliding_window.py /app/pipelines/
COPY pipelines/ema.py /app/pipelines/
COPY pipelines/kdq_tree.py /app/pipelines/

# Copy the JMX Prometheus Java agent and the exporter configuration
COPY prometheus_metrics/jmx_prometheus_javaagent-1.0.1.jar /opt/bitnami/spark/jars/
COPY prometheus_metrics/spark-jmx-exporter.yml /opt/bitnami/spark/jars/
COPY prometheus_metrics/metrics.properties /opt/bitnami/spark/conf/

# Set environment variables for Spark and Python
ENV SPARK_HOME=/opt/bitnami/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH:/app 
ENV PYSPARK_PYTHON=python3

# Install necessary Python libraries including Menelaus and prometheus-client
RUN pip install prometheus-client menelaus

CMD ["spark-submit", \
     "--conf", "spark.driver.extraJavaOptions=-javaagent:/opt/bitnami/spark/jars/jmx_prometheus_javaagent-1.0.1.jar=8000:/opt/bitnami/spark/jars/spark-jmx-exporter.yml -Dcom.sun.management.jmxremote.host=0.0.0.0", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0", \
     "/app/pyspark_streaming.py"]
