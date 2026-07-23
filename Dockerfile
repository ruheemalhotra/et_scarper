# Use an official lightweight Python base image
FROM python:3.11-slim

# Prevent Python from writing .pyc files and enable unbuffered logging
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set working directory inside the container
WORKDIR /app

# Install system dependencies required for build tools and network utils
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file first to leverage Docker layer caching
COPY requirements.txt .

# Install Python packages
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Create the directory for scraped data and ensure appropriate permissions
RUN mkdir -p /app/data

# Railway automatically injects $PORT at runtime, default to 8000 if local
ENV PORT=8000
EXPOSE ${PORT}

# Run the FastAPI server via Uvicorn
CMD uvicorn app:app --host 0.0.0.0 --port ${PORT:-8000}
