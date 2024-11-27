# producer/Dockerfile

FROM python:3.8-slim

# Set the working directory
WORKDIR /app

# Copy necessary files
COPY kafka_producer.py /app/

# Install dependencies
RUN pip install kafka-python pandas

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Run the producer script
CMD ["python", "kafka_producer.py"]
