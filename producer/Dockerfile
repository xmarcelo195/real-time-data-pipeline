FROM python:3.11-slim

WORKDIR /app

# Install system dependencies (netcat for wait-for-it.sh)
RUN apt-get update && apt-get install -y netcat-openbsd && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies
COPY requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Ensure wait-for-it.sh is executable
RUN chmod +x wait-for-it.sh

# Command is overridden by docker-compose.yml entrypoint
CMD ["python", "producer.py"]