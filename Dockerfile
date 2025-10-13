# Dockerfile for External Data Parser
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libxml2-dev \
    libxslt-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY main.py .
COPY common/ ./common/
COPY gcp_services/ ./gcp_services/
COPY BAI/ ./BAI/
COPY CAMT/ ./CAMT/
COPY CSV/ ./CSV/

# Set Python path
ENV PYTHONPATH=/app

# Environment variables
ENV GCP_PROJECT_ID=""
ENV GCP_LOCATION=""
ENV BQ_DATASET_ID=""
ENV BQ_BALANCE_TABLE_ID=""
ENV BQ_TRANSACTIONS_TABLE_ID=""
ENV KMS_KEY_RING=""

# Entrypoint
ENTRYPOINT ["python", "main.py"]