FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    rsync \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better Docker layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create volume mount point for downloads
RUN mkdir -p /downloads

# Set default environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Expose any ports if needed (none specified in this app)

# Default command - can be overridden
CMD ["python", "main.py", "--help"]