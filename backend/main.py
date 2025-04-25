# ==============================================================================
# SECTION 1: FastAPI BigQuery Job Runner + Upload/ETL Trigger
# ==============================================================================
import os
import logging
import re
from datetime import timedelta, timezone, datetime # Ensure datetime is imported
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, HTTPException, Request, APIRouter, Depends, Query, Path
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
# Make sure all necessary clients are imported
from google.cloud import bigquery, storage, pubsub_v1
from google.cloud.exceptions import NotFound, BadRequest # Specific exceptions
from google.api_core.exceptions import GoogleAPICallError # General API call errors
from google.oauth2 import service_account
import pandas as pd # Keep for potential serialization if needed, but list_rows is often better
import traceback
from dotenv import load_dotenv
import uuid # For generating unique job IDs if needed (BigQuery provides them)

# --- Load Environment Variables ---
load_dotenv()

# --- FastAPI App Initialization ---
app = FastAPI(title="BigQuery & ETL API") # Updated title
# Router specifically for BigQuery related endpoints
bq_router = APIRouter(prefix="/api/bigquery", tags=["BigQuery"])

# --- Logging Setup ---
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=log_level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger_api = logging.getLogger("uvicorn.error" if "uvicorn" in os.getenv("SERVER_SOFTWARE", "") else __name__ + "_api")
logger_api.setLevel(log_level)
logger_api.info("FastAPI application starting...")

# --- Configuration ---
API_GCP_PROJECT = os.getenv("GCP_PROJECT")
# == ADD BACK GCS/PUBSUB CONFIG ==
API_GCS_BUCKET = os.getenv("GCS_BUCKET") # MUST BE SET if using upload
API_PUBSUB_TOPIC = os.getenv("PUBSUB_TOPIC") # MUST BE SET if using ETL trigger
# ================================
API_CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
DEFAULT_JOB_TIMEOUT_SECONDS = int(os.getenv("BQ_JOB_TIMEOUT_SECONDS", 300))
DEFAULT_BQ_LOCATION = os.getenv("BQ_LOCATION", "US")
# == ADD BACK SIGNED URL CONFIG ==
SIGNED_URL_EXPIRATION_MINUTES = int(os.getenv("SIGNED_URL_EXPIRATION_MINUTES", 30))
# ================================

# --- Input Validation ---
if not API_GCP_PROJECT:
    logger_api.critical("API Error: Missing required environment variable GCP_PROJECT")
# == ADD BACK GCS/PUBSUB VALIDATION ==
if not API_GCS_BUCKET:
    logger_api.warning("API Warning: GCS_BUCKET environment variable not set. Upload URL endpoint will fail.")
if not API_PUBSUB_TOPIC:
     logger_api.warning("API Warning: PUBSUB_TOPIC environment variable not set. ETL trigger endpoint will fail.")
# ===================================
if API_CREDENTIALS_PATH and not os.path.exists(API_CREDENTIALS_PATH):
     logger_api.warning(f"API Warning: Google credentials file not found at: {API_CREDENTIALS_PATH}. Attempting ADC.")

# --- Initialize API Clients ---
api_bigquery_client: Optional[bigquery.Client] = None
# == ADD BACK STORAGE/PUBSUB CLIENTS ==
api_storage_client: Optional[storage.Client] = None
api_publisher: Optional[pubsub_v1.PublisherClient] = None
api_topic_path: Optional[str] = None
# =====================================

try:
    gcp_project_id = API_GCP_PROJECT
    creds = None
    using_adc = False

    if API_CREDENTIALS_PATH and os.path.exists(API_CREDENTIALS_PATH):
        logger_api.info(f"Initializing clients using service account key: {API_CREDENTIALS_PATH}")
        creds = service_account.Credentials.from_service_account_file(API_CREDENTIALS_PATH)
    elif gcp_project_id:
        logger_api.info("Attempting client initialization using Application Default Credentials (ADC).")
        using_adc = True
        # No explicit creds needed for ADC, clients handle it
    else:
         logger_api.error("Cannot initialize clients: GCP_PROJECT not set and no credentials file path provided.")
         # Set clients to None explicitly if initialization is impossible
         api_bigquery_client = None
         api_storage_client = None
         api_publisher = None


    if gcp_project_id: # Proceed only if project ID is available
        # Initialize BigQuery Client
        try:
            api_bigquery_client = bigquery.Client(credentials=creds, project=gcp_project_id) if not using_adc else bigquery.Client(project=gcp_project_id)
            logger_api.info(f"BigQuery Client Initialized. Project: {gcp_project_id}, Default Location: {DEFAULT_BQ_LOCATION}")
        except Exception as e_bq:
             logger_api.error(f"Failed to initialize BigQuery Client: {e_bq}", exc_info=True)
             api_bigquery_client = None

        # == INITIALIZE STORAGE CLIENT (if config exists) ==
        if API_GCS_BUCKET:
            try:
                api_storage_client = storage.Client(credentials=creds, project=gcp_project_id) if not using_adc else storage.Client(project=gcp_project_id)
                logger_api.info(f"Storage Client Initialized for bucket: {API_GCS_BUCKET}")
            except Exception as e_storage:
                 logger_api.error(f"Failed to initialize Storage Client: {e_storage}", exc_info=True)
                 api_storage_client = None
        else:
             logger_api.info("Storage client initialization skipped (GCS_BUCKET not set).")
        # ===================================================

        # == INITIALIZE PUBSUB CLIENT (if config exists) ==
        if API_PUBSUB_TOPIC:
            try:
                api_publisher = pubsub_v1.PublisherClient(credentials=creds) if not using_adc else pubsub_v1.PublisherClient()
                api_topic_path = api_publisher.topic_path(gcp_project_id, API_PUBSUB_TOPIC)
                logger_api.info(f"Pub/Sub Publisher Client Initialized for topic: {api_topic_path}")
            except Exception as e_pubsub:
                logger_api.error(f"Failed to initialize Pub/Sub Client: {e_pubsub}", exc_info=True)
                api_publisher = None
                api_topic_path = None
        else:
             logger_api.info("Pub/Sub client initialization skipped (PUBSUB_TOPIC not set).")
        # ==================================================

except Exception as e:
    logger_api.critical(f"API Error during client initialization setup: {e}", exc_info=True)
    api_bigquery_client = None
    api_storage_client = None
    api_publisher = None

# --- CORS Middleware ---
allowed_origins = os.getenv("ALLOWED_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"], # Be more specific in production: ["GET", "POST", "OPTIONS"]
    allow_headers=["*"], # Be more specific in production if needed
)
logger_api.info(f"CORS enabled for origins: {allowed_origins}")

# --- Pydantic Models ---

class TableListItem(BaseModel):
    tableId: str

class QueryRequest(BaseModel):
    sql: str = Field(..., description="The BigQuery SQL query or script to execute.")
    default_dataset: Optional[str] = Field(None, description="Default dataset for unqualified table names (e.g., 'my_project.my_dataset').")
    max_bytes_billed: Optional[int] = Field(None, description="Limit query cost.")
    use_legacy_sql: bool = Field(False, description="Use legacy SQL dialect.")
    priority: str = Field("INTERACTIVE", description="Query priority ('INTERACTIVE' or 'BATCH').")

class JobSubmitResponse(BaseModel):
    job_id: str
    state: str
    location: str
    message: str = "Job submitted successfully."

class JobStatusResponse(BaseModel):
    job_id: str
    state: str
    location: str
    statement_type: Optional[str] = None
    error_result: Optional[Dict[str, Any]] = None
    user_email: Optional[str] = None
    creation_time: Optional[datetime] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    total_bytes_processed: Optional[int] = None
    num_dml_affected_rows: Optional[int] = None

class JobResultsResponse(BaseModel):
    rows: List[Dict[str, Any]]
    total_rows_in_result_set: Optional[int] = None
    next_page_token: Optional[str] = None
    schema_: Optional[List[Dict[str, Any]]] = Field(None, alias="schema")

class TableStatsModel(BaseModel):
    rowCount: Optional[int] = None
    sizeBytes: Optional[int] = None
    lastModified: Optional[str] = None

class TableDataResponse(BaseModel):
    rows: List[Dict[str, Any]]
    totalRows: Optional[int] = None
    stats: Optional[TableStatsModel] = None

# == ADD BACK ETLRequest MODEL ==
class ETLRequest(BaseModel):
    object_name: str
# ===============================

# --- Helper Functions ---
# serialize_bq_row and serialize_bq_schema remain unchanged
def serialize_bq_row(row: bigquery.table.Row) -> Dict[str, Any]:
    """Converts a single BigQuery Row to a JSON-serializable dictionary."""
    record = {}
    for key, value in row.items():
        if isinstance(value, bytes):
            try:
                record[key] = value.decode('utf-8')
            except UnicodeDecodeError:
                record[key] = f"0x{value.hex()}"
        elif isinstance(value, (datetime, pd.Timestamp)):
            if value.tzinfo is None:
                 value = value.replace(tzinfo=timezone.utc)
            record[key] = value.isoformat()
        elif isinstance(value, list) or isinstance(value, dict):
             record[key] = value
        else:
            record[key] = value
    return record

def serialize_bq_schema(schema: List[bigquery.schema.SchemaField]) -> List[Dict[str, Any]]:
    """Converts BigQuery schema to a list of simple dictionaries."""
    return [
        {"name": field.name, "type": field.field_type, "mode": field.mode}
        for field in schema
    ]

# --- BigQuery API Endpoints (using bq_router) ---

@bq_router.post("/jobs", response_model=JobSubmitResponse, status_code=202)
async def submit_bigquery_job(req: QueryRequest):
    # ... (submit_bigquery_job function remains unchanged)
    if not api_bigquery_client:
        raise HTTPException(status_code=503, detail="BigQuery client not available.")
    if not req.sql or req.sql.isspace():
         raise HTTPException(status_code=400, detail="SQL query cannot be empty.")
    logger_api.info(f"Received job submission request. SQL: {req.sql[:100]}...")
    job_config = bigquery.QueryJobConfig(
        priority=req.priority.upper(),
        use_legacy_sql=req.use_legacy_sql
    )
    if req.default_dataset:
        try:
            project, dataset = req.default_dataset.split('.')
            job_config.default_dataset = bigquery.DatasetReference(project, dataset)
        except ValueError:
             raise HTTPException(status_code=400, detail="Invalid format for default_dataset. Use 'project.dataset'.")
    if req.max_bytes_billed is not None:
        job_config.maximum_bytes_billed = req.max_bytes_billed
    try:
        query_job = api_bigquery_client.query(req.sql, job_config=job_config, location=DEFAULT_BQ_LOCATION)
        logger_api.info(f"BigQuery Job Submitted. Job ID: {query_job.job_id}, Location: {query_job.location}, Initial State: {query_job.state}")
        return JobSubmitResponse(
            job_id=query_job.job_id,
            state=query_job.state,
            location=query_job.location
        )
    except BadRequest as e:
        logger_api.error(f"Invalid Query Syntax or Configuration: {e}", exc_info=False)
        raise HTTPException(status_code=400, detail=f"Invalid query or configuration: {str(e)}")
    except GoogleAPICallError as e:
         logger_api.error(f"Google API Call Error during job submission: {e}", exc_info=True)
         raise HTTPException(status_code=502, detail=f"Error communicating with BigQuery API: {str(e)}")
    except Exception as e:
        logger_api.error(f"Unexpected error submitting job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")

@bq_router.get("/tables", response_model=List[TableListItem])
async def list_bigquery_tables(dataset_id: str = Query(..., description="Full dataset ID (e.g., project.dataset)")):
    # ... (list_bigquery_tables function remains unchanged)
    if not api_bigquery_client:
        logger_api.error("Cannot list tables: BigQuery client not available.")
        raise HTTPException(status_code=503, detail="BigQuery client not available.")
    if not dataset_id:
         raise HTTPException(status_code=400, detail="dataset_id query parameter is required")
    logger_api.info(f"Listing tables for dataset: {dataset_id}")
    try:
        tables_iterator = api_bigquery_client.list_tables(dataset_id)
        results = [TableListItem(tableId=table.table_id) for table in tables_iterator]
        logger_api.info(f"Found {len(results)} tables in dataset {dataset_id}")
        return results
    except NotFound:
        logger_api.warning(f"Dataset not found during table list: {dataset_id}")
        raise HTTPException(status_code=404, detail=f"Dataset not found: {dataset_id}")
    except GoogleAPICallError as e:
        logger_api.error(f"Google API Call Error listing tables for {dataset_id}: {e}", exc_info=True)
        raise HTTPException(status_code=502, detail=f"Error communicating with BigQuery API: {str(e)}")
    except Exception as e:
        logger_api.error(f"Unexpected error listing tables for {dataset_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred while listing tables: {str(e)}")


@bq_router.get("/table-data", response_model=TableDataResponse)
async def get_table_data(
    dataset_id: str = Query(..., description="Full dataset ID (e.g., project.dataset)"),
    table_id:   str = Query(..., description="Table name"),
    page:       int = Query(1, ge=1, description="Page number for preview (1-based)"),
    limit:      int = Query(10, ge=1, le=100, description="Rows per page for preview")
):
    # ... (get_table_data function remains unchanged)
    if not api_bigquery_client:
        logger_api.error("Cannot get table data: BigQuery client not available.")
        raise HTTPException(status_code=503, detail="BigQuery client not available.")
    full_table_id = f"{dataset_id}.{table_id}"
    logger_api.info(f"Fetching preview data for table: {full_table_id}, page: {page}, limit: {limit}")
    try:
        table_ref = api_bigquery_client.get_table(full_table_id)
        offset = (page - 1) * limit
        rows_iterator = api_bigquery_client.list_rows(
            table_ref,
            start_index=offset,
            max_results=limit,
            timeout=60
        )
        results = [serialize_bq_row(row) for row in rows_iterator]
        total_rows = table_ref.num_rows
        stats = TableStatsModel(
            rowCount=table_ref.num_rows,
            sizeBytes=table_ref.num_bytes,
            lastModified=table_ref.modified.isoformat() if table_ref.modified else None
        )
        logger_api.info(f"Returning {len(results)} preview rows for {full_table_id} (page {page}), total rows: {total_rows}")
        return TableDataResponse(rows=results, totalRows=total_rows, stats=stats)
    except NotFound:
        logger_api.warning(f"Table not found during preview fetch: {full_table_id}")
        raise HTTPException(status_code=404, detail=f"Table not found: {full_table_id}")
    except GoogleAPICallError as e:
         logger_api.error(f"Google API Call Error fetching preview data for {full_table_id}: {e}", exc_info=True)
         raise HTTPException(status_code=502, detail=f"Error communicating with BigQuery API: {str(e)}")
    except Exception as e:
        logger_api.error(f"Unexpected error fetching preview data for {full_table_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred while fetching table preview: {str(e)}")


@bq_router.get("/jobs/{job_id}", response_model=JobStatusResponse)
async def get_bigquery_job_status(
    job_id: str = Path(..., description="The BigQuery Job ID."),
    location: str = Query(DEFAULT_BQ_LOCATION, description="The location where the job was run.")
):
    # ... (get_bigquery_job_status function remains unchanged)
    if not api_bigquery_client:
        raise HTTPException(status_code=503, detail="BigQuery client not available.")
    logger_api.debug(f"Fetching status for Job ID: {job_id}, Location: {location}")
    try:
        job = api_bigquery_client.get_job(job_id, location=location)
        error_detail = None
        if job.error_result:
            error_detail = {
                "reason": job.error_result.get("reason"),
                "location": job.error_result.get("location"),
                "message": job.error_result.get("message"),
            }
        return JobStatusResponse(
            job_id=job.job_id, state=job.state, location=job.location,
            statement_type=job.statement_type, error_result=error_detail,
            user_email=job.user_email, creation_time=job.created,
            start_time=job.started, end_time=job.ended,
            total_bytes_processed=job.total_bytes_processed,
            num_dml_affected_rows=getattr(job, 'num_dml_affected_rows', None)
        )
    except NotFound:
        logger_api.warning(f"Job not found: {job_id} in location {location}")
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found in location '{location}'.")
    except GoogleAPICallError as e:
         logger_api.error(f"Google API Call Error fetching job status: {e}", exc_info=True)
         raise HTTPException(status_code=502, detail=f"Error communicating with BigQuery API: {str(e)}")
    except Exception as e:
        logger_api.error(f"Unexpected error fetching job status for {job_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")


@bq_router.get("/jobs/{job_id}/results", response_model=JobResultsResponse)
async def get_bigquery_job_results(
    job_id: str = Path(..., description="The BigQuery Job ID."),
    location: str = Query(DEFAULT_BQ_LOCATION, description="The location where the job was run."),
    page_token: Optional[str] = Query(None, description="Token for fetching the next page of results."),
    max_results: int = Query(100, ge=1, le=1000, description="Maximum number of rows per page.")
):
    # ... (get_bigquery_job_results function remains unchanged)
    if not api_bigquery_client:
        raise HTTPException(status_code=503, detail="BigQuery client not available.")
    logger_api.debug(f"Fetching results for Job ID: {job_id}, Location: {location}, PageToken: {page_token}, MaxResults: {max_results}")
    try:
        job = api_bigquery_client.get_job(job_id, location=location)
        if job.state != 'DONE':
            raise HTTPException(status_code=400, detail=f"Job {job_id} is not complete. Current state: {job.state}")
        if job.error_result:
            error_msg = job.error_result.get('message', 'Unknown error')
            raise HTTPException(status_code=400, detail=f"Job {job_id} failed: {error_msg}")
        if not job.destination:
            logger_api.info(f"Job {job_id} completed but did not produce a destination table (e.g., DML/DDL).")
            # Return affected rows count if available
            affected_rows = getattr(job, 'num_dml_affected_rows', 0)
            return JobResultsResponse(rows=[], total_rows_in_result_set=affected_rows if affected_rows is not None else 0, schema=[])
        rows_iterator = api_bigquery_client.list_rows(
            job.destination, max_results=max_results, page_token=page_token,
            timeout=DEFAULT_JOB_TIMEOUT_SECONDS
        )
        serialized_rows = [serialize_bq_row(row) for row in rows_iterator]
        schema_info = serialize_bq_schema(rows_iterator.schema)
        return JobResultsResponse(
            rows=serialized_rows,
            total_rows_in_result_set=rows_iterator.total_rows,
            next_page_token=rows_iterator.next_page_token,
            schema=schema_info
        )
    except NotFound:
        logger_api.warning(f"Job or destination table not found for job {job_id} in location {location}")
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' or its results not found in location '{location}'. Results might expire.")
    except GoogleAPICallError as e:
         logger_api.error(f"Google API Call Error fetching job results: {e}", exc_info=True)
         raise HTTPException(status_code=502, detail=f"Error communicating with BigQuery API: {str(e)}")
    except Exception as e:
        logger_api.error(f"Unexpected error fetching job results for {job_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")


# --- ADD BACK Upload/ETL Endpoints (attached directly to app) ---

@app.get("/api/upload-url", tags=["Upload"])
async def get_upload_url(filename: str = Query(..., description="Name of the file to upload.")):
    """Generates a signed URL for client-side uploads to GCS."""
    if not api_storage_client:
        logger_api.error("Cannot generate upload URL: Storage client not available (check GCS_BUCKET config).")
        raise HTTPException(status_code=503, detail="Storage service unavailable")
    if not filename:
        raise HTTPException(status_code=400, detail="Filename parameter is required")
    if not API_GCS_BUCKET: # Double check bucket config is actually loaded
         raise HTTPException(status_code=500, detail="GCS Bucket configuration missing on server.")


    # Basic filename sanitization (more robust checks might be needed)
    clean_filename = re.sub(r"[^a-zA-Z0-9_.-]", "_", os.path.basename(filename))
    # Standardize destination prefix
    destination_blob_name = f"uploads/{clean_filename}"

    logger_api.info(f"Generating signed URL for blob: {destination_blob_name} in bucket {API_GCS_BUCKET}")

    try:
        bucket = api_storage_client.bucket(API_GCS_BUCKET)
        blob = bucket.blob(destination_blob_name)

        url = blob.generate_signed_url(
            version="v4",
            expiration=timedelta(minutes=SIGNED_URL_EXPIRATION_MINUTES),
            method="PUT",
            # content_type="application/octet-stream", # Optional: Let client set content-type header
        )
        logger_api.info(f"Signed URL generated successfully for {destination_blob_name}")
        return {"url": url, "object_name": blob.name} # Return object name for ETL trigger
    except NotFound:
         logger_api.error(f"GCS Bucket not found: {API_GCS_BUCKET}")
         raise HTTPException(status_code=404, detail=f"GCS Bucket '{API_GCS_BUCKET}' not found.")
    except GoogleAPICallError as e:
         logger_api.error(f"Google API Call Error generating signed URL for {filename}: {e}", exc_info=True)
         raise HTTPException(status_code=502, detail=f"Error communicating with GCS API: {str(e)}")
    except Exception as e:
        logger_api.error(f"Error generating signed URL for {filename}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Could not generate upload URL: {str(e)}")


@app.post("/api/trigger-etl", tags=["ETL"])
async def trigger_etl(payload: ETLRequest, request: Request):
    """Publishes the GCS object name to Pub/Sub to trigger the ETL worker."""
    if not api_publisher or not api_topic_path:
         logger_api.error("Cannot trigger ETL: Publisher client not available (check PUBSUB_TOPIC config).")
         raise HTTPException(status_code=503, detail="Messaging service unavailable")
    # Validate object_name format if needed (e.g., ensure it's in the uploads/ prefix)
    if not payload.object_name or not payload.object_name.startswith("uploads/"):
        logger_api.warning(f"Invalid object_name received for ETL trigger: {payload.object_name}")
        raise HTTPException(status_code=400, detail="Invalid object_name provided. Must start with 'uploads/'.")

    client_ip = request.client.host if request.client else "unknown"
    logger_api.info(f"Triggering ETL for object: {payload.object_name} (requested by {client_ip})")

    try:
        data = payload.object_name.encode("utf-8")
        # Publish asynchronously
        future = api_publisher.publish(api_topic_path, data=data)

        # Optional: Add callback for logging success/failure, but don't block
        def pubsub_callback(f):
             try:
                 message_id = f.result()
                 logger_api.info(f"Pub/Sub message published successfully for {payload.object_name}. Message ID: {message_id}")
             except Exception as pub_e:
                 logger_api.error(f"Failed to publish Pub/Sub message for {payload.object_name}: {pub_e}", exc_info=True)
        future.add_done_callback(pubsub_callback)

        # Return quickly
        logger_api.info(f"ETL job queued successfully for {payload.object_name}")
        return {"status": "queued", "object_name": payload.object_name}

    except GoogleAPICallError as e:
         logger_api.error(f"Google API Call Error publishing ETL trigger for {payload.object_name} to Pub/Sub: {e}", exc_info=True)
         raise HTTPException(status_code=502, detail=f"Error communicating with Pub/Sub API: {str(e)}")
    except Exception as e:
        logger_api.error(f"Error publishing ETL trigger for {payload.object_name} to Pub/Sub: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Could not trigger ETL: {str(e)}")

# --- Include BigQuery Router ---
app.include_router(bq_router) # BigQuery endpoints are under /api/bigquery/...

# --- Optional Health Check ---
@app.get("/api/health", tags=["Health"])
async def health_check():
    """Basic health check for essential services."""
    # Check components based on whether they were successfully initialized
    statuses = {}
    overall_status = "ok"

    if api_bigquery_client:
        statuses["bigquery"] = "ok"
    else:
        statuses["bigquery"] = "unavailable"
        overall_status = "unhealthy" # Make BQ essential

    # Check Storage only if bucket is configured
    if API_GCS_BUCKET:
        if api_storage_client:
            statuses["storage"] = "ok"
        else:
            statuses["storage"] = "unavailable"
            overall_status = "unhealthy" # Make Storage essential if configured

     # Check PubSub only if topic is configured
    if API_PUBSUB_TOPIC:
        if api_publisher:
            statuses["pubsub"] = "ok"
        else:
            statuses["pubsub"] = "unavailable"
            overall_status = "unhealthy" # Make PubSub essential if configured

    if overall_status == "unhealthy":
         # Return 503 if any essential component failed
        raise HTTPException(status_code=503, detail=statuses)

    return {"status": overall_status, "components": statuses}


# --- Root Endpoint ---
@app.get("/", tags=["Root"], include_in_schema=False) # Hide from OpenAPI docs if desired
def read_root():
    return {"message": f"{app.title} is running."}

# --- Uvicorn Runner (for local development) ---
if __name__ == "__main__":
    import uvicorn
    api_host = os.getenv("API_HOST", "127.0.0.1")
    api_port = int(os.getenv("API_PORT", 8000))
    logger_api.info(f"Starting Uvicorn server on http://{api_host}:{api_port}")
    uvicorn.run("main:app", host=api_host, port=api_port, reload=True)