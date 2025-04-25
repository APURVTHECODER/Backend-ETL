# ==============================================================================
# SECTION 1: FastAPI Upload Server
# ==============================================================================

from fastapi import FastAPI, File, UploadFile, HTTPException, Request,APIRouter, Depends , Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from google.cloud import storage
from google.oauth2 import service_account
from google.cloud.pubsub_v1 import PublisherClient
from datetime import timedelta, timezone # Ensure timezone aware for consistency
import os
import logging
from typing import List, Dict, Any  # Add Any here

import re
from google.cloud import bigquery
import traceback
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from typing import List, Optional
from google.cloud import bigquery
# --- Load Environment Variables ---
# Load variables for both FastAPI and Worker sections if run together
# Ensure .env file is in the directory where you run the script
load_dotenv()

# --- FastAPI App Initialization ---
app = FastAPI(title="ETL Upload API")
router = APIRouter()

api_bigquery_client = bigquery.Client()
# --- FastAPI Logging Setup ---
# Use uvicorn's logger if available, otherwise basic config
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=log_level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger_api = logging.getLogger("uvicorn.error" if "uvicorn" in os.getenv("SERVER_SOFTWARE", "") else __name__ + "_api") # More specific logger name
logger_api.setLevel(log_level)
logger_api.info("FastAPI application starting...")


# --- FastAPI Configuration ---
# Use environment variables with defaults
API_GCP_PROJECT = os.getenv("GCP_PROJECT", "your-gcp-project-id") # MUST BE SET
API_GCS_BUCKET = os.getenv("GCS_BUCKET", "your-gcs-bucket-name") # MUST BE SET
API_PUBSUB_TOPIC = os.getenv("PUBSUB_TOPIC", "your-pubsub-topic-id") # MUST BE SET
API_CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "service-account.json")
SIGNED_URL_EXPIRATION_MINUTES = int(os.getenv("SIGNED_URL_EXPIRATION_MINUTES", 30)) # Increased default

# --- Input Validation for API ---
if not all([API_GCP_PROJECT, API_GCS_BUCKET, API_PUBSUB_TOPIC]):
    logger_api.critical("API Error: Missing required environment variables (GCP_PROJECT, GCS_BUCKET, PUBSUB_TOPIC)")
    # Potentially raise error or exit if running API directly
    # raise ValueError("API configuration missing required environment variables.")
if not os.path.exists(API_CREDENTIALS_PATH):
     logger_api.critical(f"API Error: Google credentials file not found at: {API_CREDENTIALS_PATH}")
     # raise FileNotFoundError("API credentials file not found.")

# --- Initialize API Clients ---
try:
    api_creds = service_account.Credentials.from_service_account_file(API_CREDENTIALS_PATH)
    api_storage_client = storage.Client(credentials=api_creds, project=API_GCP_PROJECT)
    api_publisher = PublisherClient(credentials=api_creds)
    api_topic_path = api_publisher.topic_path(API_GCP_PROJECT, API_PUBSUB_TOPIC)
    logger_api.info(f"API Clients initialized. Project: {API_GCP_PROJECT}, Bucket: {API_GCS_BUCKET}, Topic: {API_PUBSUB_TOPIC}")
except Exception as e:
    logger_api.critical(f"API Error: Failed to initialize Google Cloud clients: {e}", exc_info=True)
    # Handle fatal error during startup
    api_storage_client = None
    api_publisher = None
    api_topic_path = None


# --- CORS Middleware ---
# Allow specific origins in production instead of "*"
allowed_origins = os.getenv("ALLOWED_ORIGINS", "*").split(",") # Read from env, split by comma
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
logger_api.info(f"CORS enabled for origins: {allowed_origins}")

# --- API Endpoints ---

@app.get("/api/health")
async def health_check():
    """Simple health check endpoint."""
    logger_api.debug("Health check endpoint called")
    # Add checks for client initialization if needed
    if not api_publisher or not api_storage_client:
         raise HTTPException(status_code=503, detail="API clients not initialized")
    return {"status": "ok"}

@app.get("/api/upload-url")
async def get_upload_url(filename: str):
    """Generates a signed URL for client-side uploads."""
    if not api_storage_client:
        logger_api.error("Cannot generate upload URL: Storage client not available.")
        raise HTTPException(status_code=503, detail="Storage service unavailable")
    if not filename:
        raise HTTPException(status_code=400, detail="Filename parameter is required")

    # Basic filename sanitization (more robust checks might be needed)
    clean_filename = re.sub(r"[^a-zA-Z0-9_.-]", "_", os.path.basename(filename))
    destination_blob_name = f"uploads/{clean_filename}" # Store in 'uploads/' prefix

    logger_api.info(f"Generating signed URL for blob: {destination_blob_name}")

    try:
        bucket = api_storage_client.bucket(API_GCS_BUCKET)
        blob = bucket.blob(destination_blob_name)

        # Generate Signed URL
        # Use recommended content_type based on expected file types if possible, or allow client to set
        # application/octet-stream is a safe fallback if type is unknown/variable
        url = blob.generate_signed_url(
            version="v4",
            expiration=timedelta(minutes=SIGNED_URL_EXPIRATION_MINUTES),
            method="PUT",
            # content_type="application/octet-stream", # Let client specify exact type for better GCS handling
            # Optionally add headers={'x-goog-resumable': 'start'} for resumable if client supports it
        )
        logger_api.info(f"Signed URL generated successfully for {destination_blob_name}")
        return {"url": url, "object_name": blob.name}
    except Exception as e:
        logger_api.error(f"Error generating signed URL for {filename}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Could not generate upload URL: {e}")


class ETLRequest(BaseModel):
    object_name: str

@app.post("/api/trigger-etl")
async def trigger_etl(payload: ETLRequest, request: Request):
    """Publishes the GCS object name to Pub/Sub to trigger the ETL worker."""
    if not api_publisher or not api_topic_path:
         logger_api.error("Cannot trigger ETL: Publisher client not available.")
         raise HTTPException(status_code=503, detail="Messaging service unavailable")
    if not payload.object_name or not payload.object_name.startswith("uploads/"):
        logger_api.warning(f"Invalid object_name received: {payload.object_name}")
        raise HTTPException(status_code=400, detail="Invalid object_name provided")

    client_ip = request.client.host if request.client else "unknown"
    logger_api.info(f"Triggering ETL for object: {payload.object_name} (requested by {client_ip})")

    try:
        data = payload.object_name.encode("utf-8")
        # Publish asynchronously
        future = api_publisher.publish(api_topic_path, data=data)

        # Optional: Wait briefly for result, but don't block excessively in API
        # Consider using background tasks for robustness if publish fails sometimes
        # message_id = future.result(timeout=10) # Wait max 10 seconds
        future.add_done_callback(lambda f: logger_api.info(f"Pub/Sub publish future done for {payload.object_name}. Result/Exc: {f.result() if not f.exception() else f.exception()}"))

        # Return quickly, don't wait for future.result() which blocks
        logger_api.info(f"ETL job queued successfully for {payload.object_name}")
        return {"status": "queued", "object_name": payload.object_name} # Return message_id if waited for result
    except Exception as e:
        logger_api.error(f"Error publishing ETL trigger for {payload.object_name} to Pub/Sub: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Could not trigger ETL: {e}")
    

class TableReference(BaseModel):
    tableId: str


@router.get("/api/bigquery/tables", response_model=List[TableReference])
def list_bigquery_tables(dataset_id: str):
    if not api_bigquery_client:
        raise HTTPException(503, "BigQuery client not available")
    if not dataset_id:
        raise HTTPException(400, "dataset_id parameter is required")

    try:
        items = api_bigquery_client.list_tables(dataset_id)
        # Only pull table_id, skip fetching full Table objects
        return [TableReference(tableId=tbl.table_id) for tbl in items]
    except Exception as e:
        # handle dataset-not-found differently if you like
        raise HTTPException(5)
    # --- Include the router in the main app ---


@router.get("/api/bigquery/table-data", response_model=List[Dict[str, Any]])
def get_table_data(
    dataset_id: str = Query(..., alias="dataset_id"),
    table_id:   str = Query(..., alias="table_id"),
    limit:      int = Query(50, description="Max number of rows to return"),
):
    """
    Fetch up to `limit` rows from the given BigQuery table.
    """
    try:
        full_table = f"{dataset_id}.{table_id}"
        table_ref  = api_bigquery_client.get_table(full_table)
        df         = api_bigquery_client.list_rows(table_ref, max_results=limit).to_dataframe()
        return df.to_dict(orient="records")
    except Exception as e:
        logger_api.error(f"Error getting data for {full_table}: {e}", exc_info=True)
        raise HTTPException(500, f"Could not fetch table data: {e}")




class QueryRequest(BaseModel):
    sql: str = Field(..., description="Only SELECT queries")

@router.post("/api/bigquery/query", response_model=List[Dict[str,Any]])
def run_sql(req: QueryRequest):
    sql = req.sql.strip()
    # only allow SELECT
    if not sql.lower().startswith("select"):
        raise HTTPException(400, "Only SELECT queries allowed")
    try:
        # DEBUG: print the SQL to your console
        print("Running SQL:", sql)
        job = api_bigquery_client.query(sql)      # ← use your BigQuery client
        records = [dict(row) for row in job.result()]
        return records
    except Exception as e:
        print("Query error:", e)
        raise HTTPException(500, f"Query failed: {e}")





app.include_router(router)

# Optional: Add a root endpoint for basic testing
@app.get("/")
def read_root():
    return {"message": "ETL Upload API is running."}