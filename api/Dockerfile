# Use official Python 3.12 slim image as base
FROM python:3.12-slim

# Set working directory inside the container
WORKDIR /app

# Install Poetry package manager
RUN pip install --no-cache-dir poetry

# Copy poetry files first for better layer caching
COPY pyproject.toml poetry.lock* ./

# Configure poetry and install dependencies
RUN poetry config virtualenvs.create false \
 && poetry install --no-interaction --no-ansi --no-root

# Copy the rest of the application
COPY . .


# Expose the port your app runs on
EXPOSE 8888

# Create a non-root user for security
RUN useradd --create-home --shell /bin/bash app \
    && chown -R app:app /app
USER app

# Use exec form for better signal handling
CMD ["python", "-m", "uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8888"]