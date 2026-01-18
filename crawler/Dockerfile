# Use official Python 3.12 slim image as base
FROM python:3.12-slim

# Set working directory inside the container
WORKDIR /app

# Install Poetry package manager
RUN pip install --no-cache-dir poetry

# Copy the entire application first
COPY . /app

# Disable virtual environments and install dependencies only
RUN poetry config virtualenvs.create false \
 && poetry install --no-interaction --no-ansi --no-root

# Default command to run your scheduler script
CMD ["python", "scheduler.py"]