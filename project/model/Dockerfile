# model/Dockerfile

FROM python:3.8-slim

# Set the working directory
WORKDIR /app

# Copy the model script
COPY model.py /app/

# Install dependencies
RUN pip install elasticsearch==7.17.9 pandas scikit-learn joblib

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Run the model script
CMD ["python", "model.py"]
