# Use official Python image
FROM python:3.10-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PORT=8000

# Copy only requirements first to leverage Docker cache
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Set environment variable for Firebase Admin SDK key path


# Copy the rest
COPY . .
ENV FIREBASE_ADMIN_SDK_KEY_PATH=/path/in/container/to/serviceAccountKey.json
# Run FastAPI on port 8080
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]