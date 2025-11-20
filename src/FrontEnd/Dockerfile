FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .
COPY ../../.env

# Expose port (will be read from config.yml)
EXPOSE 8000

# Run the application
CMD ["python", "-m", "app.main"]
