# Use the official Python 3.10 slim image (based on Debian Bookworm - good ARM64 support)
FROM python:3.10-slim-bookworm

# Set environment variable to avoid interactive prompts during package installations
ENV DEBIAN_FRONTEND=noninteractive

WORKDIR /app

# Install system dependencies:
# - ca-certificates: For secure connections
# - chromium: The browser (ARM64 compatible version from Debian repos)
# - chromium-driver: The WebDriver for Chromium (ARM64 compatible)
# - fonts-liberation: Recommended fonts for headless rendering
# Basic build tools (might be needed by some pip packages indirectly, though often not for wheels)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        fonts-liberation \
        chromium \
        chromium-driver \
        # Add build-essential temporarily if any pip install fails needing compilation
        # build-essential \
    && \
    # --- Install Python Libraries ---
    pip install --upgrade pip && \
    pip install --no-cache-dir pandas numpy selenium && \
    # --- Clean Up ---
    # Remove build-essential if it was temporarily installed
    # apt-get purge -y --auto-remove build-essential && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV PYTHONPATH=/app

COPY requirements.txt /app/requirements.txt

RUN pip install --upgrade pip
RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt

RUN mkdir -p /app/src

COPY ./src/data_acquisition.py /app/src/data_acquisition.py
COPY ./src/google_cloud_storage_manager.py /app/src/google_cloud_storage_manager.py
COPY ./src/__init__.py /app/src/__init__.py
COPY .env /app/src/.env

CMD ["python", "src/data_acquisition.py"]