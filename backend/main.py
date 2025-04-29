# ==============================================================================
# SECTION 1: FastAPI BigQuery Job Runner + Upload/ETL Trigger + AI Features
# ==============================================================================
import os
import logging
import re
from datetime import timedelta, timezone, datetime
from typing import List, Dict, Any, Optional
import traceback
import uuid

# FastAPI and Pydantic
from fastapi import FastAPI, HTTPException, Request, APIRouter, Depends, Query, Path
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# Google Cloud Libraries
from google.cloud import bigquery, storage, pubsub_v1
from google.cloud.exceptions import NotFound, BadRequest
from google.api_core.exceptions import GoogleAPICallError, DeadlineExceeded
from google.oauth2 import service_account
import google.generativeai as genai # <-- Import Gemini library

# Utilities
from dotenv import load_dotenv
import pandas as pd

# --- Load Environment Variables ---
load_dotenv()

# --- FastAPI App Initialization ---
app = FastAPI(title="Intelligent BigQuery & ETL API") # Updated title
# Separate router for BigQuery related endpoints
bq_router = APIRouter(prefix="/api/bigquery", tags=["BigQuery"])

# --- Logging Setup ---
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=log_level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger_api = logging.getLogger("uvicorn.error" if "uvicorn" in os.getenv("SERVER_SOFTWARE", "") else __name__ + "_api")
logger_api.setLevel(log_level)
logger_api.info("FastAPI application starting...")

# --- Configuration ---
API_GCP_PROJECT = os.getenv("GCP_PROJECT")
API_GCS_BUCKET = os.getenv("GCS_BUCKET")
API_PUBSUB_TOPIC = os.getenv("PUBSUB_TOPIC")
API_CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
DEFAULT_JOB_TIMEOUT_SECONDS = int(os.getenv("BQ_JOB_TIMEOUT_SECONDS", 300))
DEFAULT_BQ_LOCATION = os.getenv("BQ_LOCATION", "US")
SIGNED_URL_EXPIRATION_MINUTES = int(os.getenv("SIGNED_URL_EXPIRATION_MINUTES", 30))
# === GEMINI API KEY ===
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY") # Get API key from environment
# ======================

# --- Input Validation ---
if not API_GCP_PROJECT:
    logger_api.critical("API Error: Missing required environment variable GCP_PROJECT")
# Optional checks for GCS/PubSub/Credentials/Gemini Key
if not API_GCS_BUCKET: logger_api.warning("API Warning: GCS_BUCKET not set. Upload URL endpoint will fail.")
if not API_PUBSUB_TOPIC: logger_api.warning("API Warning: PUBSUB_TOPIC not set. ETL trigger endpoint will fail.")
if API_CREDENTIALS_PATH and not os.path.exists(API_CREDENTIALS_PATH): logger_api.warning(f"API Warning: Credentials file not found at: {API_CREDENTIALS_PATH}. Attempting ADC.")
# === Check for Gemini Key ===
if not GEMINI_API_KEY: logger_api.warning("API Warning: GEMINI_API_KEY not set. NL-to-SQL endpoint will fail.")
# ==========================

# --- Initialize API Clients ---
api_bigquery_client: Optional[bigquery.Client] = None
api_storage_client: Optional[storage.Client] = None
api_publisher: Optional[pubsub_v1.PublisherClient] = None
api_topic_path: Optional[str] = None
# === Configure Gemini ===
if GEMINI_API_KEY:
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        logger_api.info("Gemini API configured successfully.")
    except Exception as e_gemini_config:
        logger_api.error(f"Failed to configure Gemini API: {e_gemini_config}")
        GEMINI_API_KEY = None # Prevent attempts to use it if config failed
else:
     logger_api.info("Gemini client initialization skipped (GEMINI_API_KEY not set).")
# ========================

try:
    # ... (Existing client initialization logic remains the same) ...
    gcp_project_id = API_GCP_PROJECT
    creds = None
    using_adc = False
    if API_CREDENTIALS_PATH and os.path.exists(API_CREDENTIALS_PATH):
        creds = service_account.Credentials.from_service_account_file(API_CREDENTIALS_PATH)
    elif gcp_project_id: using_adc = True
    else: logger_api.error("Cannot initialize clients: GCP_PROJECT not set and no credentials file path provided.")

    if gcp_project_id:
        try:
            api_bigquery_client = bigquery.Client(credentials=creds, project=gcp_project_id) if not using_adc else bigquery.Client(project=gcp_project_id)
            logger_api.info(f"BigQuery Client Initialized. Project: {gcp_project_id}, Default Location: {DEFAULT_BQ_LOCATION}")
        except Exception as e_bq: logger_api.error(f"Failed to initialize BigQuery Client: {e_bq}", exc_info=True); api_bigquery_client = None
        if API_GCS_BUCKET:
            try:
                api_storage_client = storage.Client(credentials=creds, project=gcp_project_id) if not using_adc else storage.Client(project=gcp_project_id)
                logger_api.info(f"Storage Client Initialized for bucket: {API_GCS_BUCKET}")
            except Exception as e_storage: logger_api.error(f"Failed to initialize Storage Client: {e_storage}", exc_info=True); api_storage_client = None
        else: logger_api.info("Storage client initialization skipped (GCS_BUCKET not set).")
        if API_PUBSUB_TOPIC:
            try:
                api_publisher = pubsub_v1.PublisherClient(credentials=creds) if not using_adc else pubsub_v1.PublisherClient()
                api_topic_path = api_publisher.topic_path(gcp_project_id, API_PUBSUB_TOPIC)
                logger_api.info(f"Pub/Sub Publisher Client Initialized for topic: {api_topic_path}")
            except Exception as e_pubsub: logger_api.error(f"Failed to initialize Pub/Sub Client: {e_pubsub}", exc_info=True); api_publisher = None; api_topic_path = None
        else: logger_api.info("Pub/Sub client initialization skipped (PUBSUB_TOPIC not set).")

except Exception as e:
    logger_api.critical(f"API Error during client initialization setup: {e}", exc_info=True)
    api_bigquery_client = None; api_storage_client = None; api_publisher = None

# --- CORS Middleware ---
# ... (CORS middleware setup remains the same) ...
allowed_origins = os.getenv("ALLOWED_ORIGINS", "*").split(",")
app.add_middleware(CORSMiddleware, allow_origins=allowed_origins, allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
logger_api.info(f"CORS enabled for origins: {allowed_origins}")

# --- Pydantic Models ---
# ... (Existing models: TableListItem, QueryRequest, JobSubmitResponse, JobStatusResponse, JobResultsResponse, TableStatsModel, TableDataResponse, ETLRequest) ...
class TableListItem(BaseModel): tableId: str
class QueryRequest(BaseModel): sql: str; default_dataset: Optional[str] = None; max_bytes_billed: Optional[int] = None; use_legacy_sql: bool = False; priority: str = "INTERACTIVE"
class JobSubmitResponse(BaseModel): job_id: str; state: str; location: str; message: str = "Job submitted successfully."
class JobStatusResponse(BaseModel): job_id: str; state: str; location: str; statement_type: Optional[str] = None; error_result: Optional[Dict[str, Any]] = None; user_email: Optional[str] = None; creation_time: Optional[datetime] = None; start_time: Optional[datetime] = None; end_time: Optional[datetime] = None; total_bytes_processed: Optional[int] = None; num_dml_affected_rows: Optional[int] = None
class JobResultsResponse(BaseModel): rows: List[Dict[str, Any]]; total_rows_in_result_set: Optional[int] = None; next_page_token: Optional[str] = None; schema_: Optional[List[Dict[str, Any]]] = Field(None, alias="schema")
class TableStatsModel(BaseModel): rowCount: Optional[int] = None; sizeBytes: Optional[int] = None; lastModified: Optional[str] = None
class TableDataResponse(BaseModel): rows: List[Dict[str, Any]]; totalRows: Optional[int] = None; stats: Optional[TableStatsModel] = None
class ETLRequest(BaseModel): object_name: str

# === NEW Pydantic Models for AI Features ===
class ColumnInfo(BaseModel):
    name: str
    type: str
    mode: str

class TableSchema(BaseModel):
    table_id: str
    columns: List[ColumnInfo]

class SchemaResponse(BaseModel):
    dataset_id: str
    tables: List[TableSchema]

class NLQueryRequest(BaseModel):
    prompt: str = Field(..., description="Natural language query from the user.")
    dataset_id: str = Field(..., description="The dataset context for the query.")
    # Optional: Could add max_tokens, temperature etc. if needed

class NLQueryResponse(BaseModel):
    generated_sql: Optional[str] = None
    error: Optional[str] = None
# =========================================

# --- Helper Functions ---
# ... (serialize_bq_row, serialize_bq_schema remain unchanged) ...
def serialize_bq_row(row: bigquery.table.Row) -> Dict[str, Any]:
    record = {}
    for key, value in row.items():
        if isinstance(value, bytes):
            try: record[key] = value.decode('utf-8')
            except UnicodeDecodeError: record[key] = f"0x{value.hex()}"
        elif isinstance(value, (datetime, pd.Timestamp)):
            if value.tzinfo is None: value = value.replace(tzinfo=timezone.utc)
            record[key] = value.isoformat()
        elif isinstance(value, list) or isinstance(value, dict): record[key] = value
        else: record[key] = value
    return record

def serialize_bq_schema(schema: List[bigquery.schema.SchemaField]) -> List[Dict[str, Any]]:
    return [{"name": field.name, "type": field.field_type, "mode": field.mode} for field in schema]


# --- Helper to fetch schema (used by /schema and /nl2sql) ---
def get_dataset_schema(dataset_id: str) -> List[TableSchema]:
    """Fetches schema for all tables in a dataset."""
    if not api_bigquery_client:
        raise HTTPException(status_code=503, detail="BigQuery client not available.")
    logger_api.info(f"Fetching schema for dataset: {dataset_id}")
    table_schemas: List[TableSchema] = []
    try:
        tables_iterator = api_bigquery_client.list_tables(dataset_id)
        for tbl_item in tables_iterator:
            try:
                full_table_id = f"{tbl_item.project}.{tbl_item.dataset_id}.{tbl_item.table_id}"
                table = api_bigquery_client.get_table(full_table_id)
                columns = [
                    ColumnInfo(name=field.name, type=field.field_type, mode=field.mode)
                    for field in table.schema
                ]
                table_schemas.append(TableSchema(table_id=tbl_item.table_id, columns=columns))
            except NotFound:
                 logger_api.warning(f"Table {full_table_id} not found while fetching schema, skipping.")
            except Exception as e_get_table:
                 logger_api.error(f"Error fetching schema for table {full_table_id}: {e_get_table}", exc_info=True)
                 # Optionally append a partial schema or skip
        logger_api.info(f"Fetched schema for {len(table_schemas)} tables in {dataset_id}")
        return table_schemas
    except NotFound:
        logger_api.warning(f"Dataset not found while fetching schema: {dataset_id}")
        raise HTTPException(status_code=404, detail=f"Dataset not found: {dataset_id}")
    except Exception as e:
        logger_api.error(f"Error listing tables for schema fetch {dataset_id}: {e}", exc_info=True)
        # Reraise or handle differently, maybe return partial results?
        raise HTTPException(status_code=500, detail=f"Could not fetch dataset schema: {str(e)}")


# === NEW Endpoint: Get Dataset Schema ===
@bq_router.get("/schema", response_model=SchemaResponse)
async def get_bigquery_dataset_schema(
    dataset_id: str = Query(..., description="Full dataset ID (e.g., project.dataset)")
):
    """Retrieves the schema (table names, column names, types) for all tables in a dataset."""
    # Consider adding caching here later for performance (e.g., using Redis or in-memory cache)
    try:
        table_schemas = get_dataset_schema(dataset_id)
        return SchemaResponse(dataset_id=dataset_id, tables=table_schemas)
    except HTTPException as http_exc:
        # Re-raise HTTP exceptions directly
        raise http_exc
    except Exception as e:
        # Catch unexpected errors from helper
        logger_api.error(f"Unexpected error in get_dataset_schema endpoint: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to retrieve schema: {str(e)}")
# ========================================


# === NEW Endpoint: Natural Language to SQL ===
@bq_router.post("/nl2sql", response_model=NLQueryResponse)
async def natural_language_to_sql(req: NLQueryRequest):
    """Converts a natural language prompt into a BigQuery SQL query using Gemini."""
    if not api_bigquery_client:
        raise HTTPException(status_code=503, detail="BigQuery client not available.")
    if not GEMINI_API_KEY:
         raise HTTPException(status_code=503, detail="NL-to-SQL service not configured (missing API key).")
    if not req.prompt or req.prompt.isspace():
         raise HTTPException(status_code=400, detail="Natural language prompt cannot be empty.")

    logger_api.info(f"Received NL-to-SQL request for dataset '{req.dataset_id}'. Prompt: {req.prompt[:100]}...")

    try:
        # 1. Fetch the relevant schema
        # Potential optimization: Only fetch schema for tables mentioned in prompt if possible?
        # For now, fetch schema for the whole dataset provided.
        table_schemas = get_dataset_schema(req.dataset_id)
        if not table_schemas:
             raise HTTPException(status_code=404, detail=f"No tables found or schema could not be retrieved for dataset {req.dataset_id}.")

        # 2. Construct the prompt for Gemini
        schema_string = "\n".join(
            [
                f"Table: {ts.table_id} (Columns: {', '.join([f'{c.name} {c.type}' for c in ts.columns])})"
                for ts in table_schemas
            ]
        )

        # Carefully crafted prompt instructions
        prompt_template = f"""You are an expert BigQuery SQL generator. Your task is to generate a *single*, valid, and executable BigQuery SQL query based on the user's natural language request and the provided database schema.

Database Schema (in dataset `{req.dataset_id}`):
{schema_string}

User Request: "{req.prompt}"

Instructions:
1.  Generate ONLY a valid BigQuery SQL `SELECT` query. Do NOT generate any other statement types (INSERT, UPDATE, DELETE, DDL).
2.  Ensure the query targets tables within the `{req.dataset_id}` dataset using the format `project_id.dataset_id.table_id` (the project_id is '{API_GCP_PROJECT}'). Example: `{API_GCP_PROJECT}.{req.dataset_id}.YourTableName`.
3.  Pay close attention to column names and types provided in the schema. Use correct SQL syntax for BigQuery (e.g., backticks for table/column names if necessary).
4.  If the request is ambiguous or requires information not present in the schema, generate a query that makes reasonable assumptions OR state that you cannot fulfill the request accurately. Do NOT ask clarifying questions.
5.  Output *only* the generated SQL query, without any introductory text, explanations, or markdown formatting (like ```sql ... ```).

Generated BigQuery SQL Query:
"""
        logger_api.debug(f"Gemini Prompt:\n{prompt_template}")

        # 3. Call the Gemini API
        model = genai.GenerativeModel('gemini-1.5-flash-latest') # Or specify a newer/different model
        # Configure safety settings if needed (e.g., block harmful content)
        # safety_settings = [...]
        response = await model.generate_content_async(
             prompt_template,
             # safety_settings=safety_settings,
             generation_config=genai.types.GenerationConfig(
                 # candidate_count=1, # We only want one best query
                 # stop_sequences=[';'], # Optional: might help ensure single query
                 # max_output_tokens=1024, # Limit output size
                 temperature=0.2 # Lower temperature for more deterministic SQL generation
             )
        )

        # 4. Parse and Clean the Response
        logger_api.debug(f"Gemini Raw Response: {response.text[:500]}...") # Log truncated response
        generated_sql = response.text.strip()

        # Optional: Basic cleaning (remove markdown backticks if present)
        if generated_sql.startswith("```sql"):
            generated_sql = generated_sql[len("```sql"):].strip()
        if generated_sql.endswith("```"):
             generated_sql = generated_sql[:-len("```")].strip()
        # Optional: Add more robust validation/parsing if needed

        if not generated_sql or not generated_sql.lower().strip().startswith("select"):
             logger_api.warning(f"Gemini did not return a valid SELECT query. Response: {generated_sql}")
             # Return an error or the potentially non-SQL response for debugging
             # return NLQueryResponse(error=f"Could not generate a valid SELECT query. LLM response: {generated_sql}")
             # Or try to return the SQL even if not SELECT, relying on BQ permissions
             return NLQueryResponse(generated_sql=generated_sql)


        logger_api.info(f"NL-to-SQL successful. Generated SQL: {generated_sql[:100]}...")
        return NLQueryResponse(generated_sql=generated_sql)

    except DeadlineExceeded:
         logger_api.error("Gemini API call timed out.")
         raise HTTPException(status_code=504, detail="NL-to-SQL generation timed out.")
    except Exception as e:
        logger_api.error(f"Error during NL-to-SQL generation: {e}", exc_info=True)
        # Check for specific Gemini errors if the library provides them
        # if isinstance(e, genai.types.BlockedPromptException):
        #     raise HTTPException(status_code=400, detail="Prompt blocked due to safety settings.")
        # if isinstance(e, genai.types.StopCandidateException):
        #      raise HTTPException(status_code=500, detail="NL-to-SQL generation stopped unexpectedly.")
        raise HTTPException(status_code=500, detail=f"Failed to generate SQL from natural language: {str(e)}")

# ===========================================


# --- Existing BigQuery Endpoints (Jobs, Status, Results, Tables, Table Data) ---
# ... (bq_router.post("/jobs", ...), bq_router.get("/tables", ...), etc. remain unchanged) ...
# Make sure they use the correct client `api_bigquery_client`
@bq_router.post("/jobs", response_model=JobSubmitResponse, status_code=202)
async def submit_bigquery_job(req: QueryRequest):
    if not api_bigquery_client: raise HTTPException(503, "BigQuery client not available.")
    # ... (rest of submit_bigquery_job) ...
    if not req.sql or req.sql.isspace(): raise HTTPException(400, "SQL query cannot be empty.")
    logger_api.info(f"Received job submission request. SQL: {req.sql[:100]}...")
    job_config = bigquery.QueryJobConfig(priority=req.priority.upper(), use_legacy_sql=req.use_legacy_sql)
    if req.default_dataset:
        try: project, dataset = req.default_dataset.split('.'); job_config.default_dataset = bigquery.DatasetReference(project, dataset)
        except ValueError: raise HTTPException(400, "Invalid format for default_dataset. Use 'project.dataset'.")
    if req.max_bytes_billed is not None: job_config.maximum_bytes_billed = req.max_bytes_billed
    try:
        query_job = api_bigquery_client.query(req.sql, job_config=job_config, location=DEFAULT_BQ_LOCATION)
        logger_api.info(f"BigQuery Job Submitted. Job ID: {query_job.job_id}, Location: {query_job.location}, Initial State: {query_job.state}")
        return JobSubmitResponse(job_id=query_job.job_id, state=query_job.state, location=query_job.location)
    except BadRequest as e: logger_api.error(f"Invalid Query Syntax or Configuration: {e}", exc_info=False); raise HTTPException(400, f"Invalid query or configuration: {str(e)}")
    except GoogleAPICallError as e: logger_api.error(f"Google API Call Error during job submission: {e}", exc_info=True); raise HTTPException(502, f"Error communicating with BigQuery API: {str(e)}")
    except Exception as e: logger_api.error(f"Unexpected error submitting job: {e}", exc_info=True); raise HTTPException(500, f"An unexpected error occurred: {str(e)}")

@bq_router.get("/tables", response_model=List[TableListItem])
async def list_bigquery_tables(dataset_id: str = Query(..., description="Full dataset ID (e.g., project.dataset)")):
    if not api_bigquery_client: raise HTTPException(503, "BigQuery client not available.")
    # ... (rest of list_bigquery_tables) ...
    if not dataset_id: raise HTTPException(400, "dataset_id query parameter is required")
    logger_api.info(f"Listing tables for dataset: {dataset_id}")
    try:
        tables_iterator = api_bigquery_client.list_tables(dataset_id)
        results = [TableListItem(tableId=table.table_id) for table in tables_iterator]
        logger_api.info(f"Found {len(results)} tables in dataset {dataset_id}")
        return results
    except NotFound: logger_api.warning(f"Dataset not found during table list: {dataset_id}"); raise HTTPException(404, f"Dataset not found: {dataset_id}")
    except GoogleAPICallError as e: logger_api.error(f"Google API Call Error listing tables for {dataset_id}: {e}", exc_info=True); raise HTTPException(502, f"Error communicating with BigQuery API: {str(e)}")
    except Exception as e: logger_api.error(f"Unexpected error listing tables for {dataset_id}: {e}", exc_info=True); raise HTTPException(500, f"An unexpected error occurred while listing tables: {str(e)}")

@bq_router.get("/table-data", response_model=TableDataResponse)
async def get_table_data(dataset_id: str = Query(..., description="Full dataset ID"), table_id: str = Query(..., description="Table name"), page: int = Query(1, ge=1), limit: int = Query(10, ge=1, le=100)):
    if not api_bigquery_client: raise HTTPException(503, "BigQuery client not available.")
    # ... (rest of get_table_data) ...
    full_table_id = f"{dataset_id}.{table_id}"
    logger_api.info(f"Fetching preview data for table: {full_table_id}, page: {page}, limit: {limit}")
    try:
        table_ref = api_bigquery_client.get_table(full_table_id)
        offset = (page - 1) * limit
        rows_iterator = api_bigquery_client.list_rows(table_ref, start_index=offset, max_results=limit, timeout=60)
        results = [serialize_bq_row(row) for row in rows_iterator]
        total_rows = table_ref.num_rows
        stats = TableStatsModel(rowCount=table_ref.num_rows, sizeBytes=table_ref.num_bytes, lastModified=table_ref.modified.isoformat() if table_ref.modified else None)
        logger_api.info(f"Returning {len(results)} preview rows for {full_table_id} (page {page}), total rows: {total_rows}")
        return TableDataResponse(rows=results, totalRows=total_rows, stats=stats)
    except NotFound: logger_api.warning(f"Table not found during preview fetch: {full_table_id}"); raise HTTPException(404, f"Table not found: {full_table_id}")
    except GoogleAPICallError as e: logger_api.error(f"Google API Call Error fetching preview data for {full_table_id}: {e}", exc_info=True); raise HTTPException(502, f"Error communicating with BigQuery API: {str(e)}")
    except Exception as e: logger_api.error(f"Unexpected error fetching preview data for {full_table_id}: {e}", exc_info=True); raise HTTPException(500, f"An unexpected error occurred while fetching table preview: {str(e)}")

@bq_router.get("/jobs/{job_id}", response_model=JobStatusResponse)
async def get_bigquery_job_status(job_id: str = Path(...), location: str = Query(DEFAULT_BQ_LOCATION)):
    if not api_bigquery_client: raise HTTPException(503, "BigQuery client not available.")
    # ... (rest of get_bigquery_job_status) ...
    logger_api.debug(f"Fetching status for Job ID: {job_id}, Location: {location}")
    try:
        job = api_bigquery_client.get_job(job_id, location=location)
        error_detail = None
        if job.error_result: error_detail = {"reason": job.error_result.get("reason"), "location": job.error_result.get("location"), "message": job.error_result.get("message")}
        return JobStatusResponse(job_id=job.job_id, state=job.state, location=job.location, statement_type=job.statement_type, error_result=error_detail, user_email=job.user_email, creation_time=job.created, start_time=job.started, end_time=job.ended, total_bytes_processed=job.total_bytes_processed, num_dml_affected_rows=getattr(job, 'num_dml_affected_rows', None))
    except NotFound: logger_api.warning(f"Job not found: {job_id} in location {location}"); raise HTTPException(404, f"Job '{job_id}' not found in location '{location}'.")
    except GoogleAPICallError as e: logger_api.error(f"Google API Call Error fetching job status: {e}", exc_info=True); raise HTTPException(502, f"Error communicating with BigQuery API: {str(e)}")
    except Exception as e: logger_api.error(f"Unexpected error fetching job status for {job_id}: {e}", exc_info=True); raise HTTPException(500, f"An unexpected error occurred: {str(e)}")

@bq_router.get("/jobs/{job_id}/results", response_model=JobResultsResponse)
async def get_bigquery_job_results(job_id: str = Path(...), location: str = Query(DEFAULT_BQ_LOCATION), page_token: Optional[str] = Query(None), max_results: int = Query(100, ge=1, le=1000)):
    if not api_bigquery_client: raise HTTPException(503, "BigQuery client not available.")
    # ... (rest of get_bigquery_job_results) ...
    logger_api.debug(f"Fetching results for Job ID: {job_id}, Location: {location}, PageToken: {page_token}, MaxResults: {max_results}")
    try:
        job = api_bigquery_client.get_job(job_id, location=location)
        if job.state != 'DONE': raise HTTPException(400, f"Job {job_id} is not complete. Current state: {job.state}")
        if job.error_result: error_msg = job.error_result.get('message', 'Unknown error'); raise HTTPException(400, f"Job {job_id} failed: {error_msg}")
        if not job.destination:
            logger_api.info(f"Job {job_id} completed but did not produce a destination table (e.g., DML/DDL).")
            affected_rows = getattr(job, 'num_dml_affected_rows', 0); return JobResultsResponse(rows=[], total_rows_in_result_set=affected_rows if affected_rows is not None else 0, schema=[])
        rows_iterator = api_bigquery_client.list_rows(job.destination, max_results=max_results, page_token=page_token, timeout=DEFAULT_JOB_TIMEOUT_SECONDS)
        serialized_rows = [serialize_bq_row(row) for row in rows_iterator]
        schema_info = serialize_bq_schema(rows_iterator.schema)
        return JobResultsResponse(rows=serialized_rows, total_rows_in_result_set=rows_iterator.total_rows, next_page_token=rows_iterator.next_page_token, schema=schema_info)
    except NotFound: logger_api.warning(f"Job or destination table not found for job {job_id} in location {location}"); raise HTTPException(404, f"Job '{job_id}' or its results not found in location '{location}'. Results might expire.")
    except GoogleAPICallError as e: logger_api.error(f"Google API Call Error fetching job results: {e}", exc_info=True); raise HTTPException(502, f"Error communicating with BigQuery API: {str(e)}")
    except Exception as e: logger_api.error(f"Unexpected error fetching job results for {job_id}: {e}", exc_info=True); raise HTTPException(500, f"An unexpected error occurred: {str(e)}")


# --- Upload/ETL Endpoints (attached directly to app) ---
# ... (app.get("/api/upload-url", ...), app.post("/api/trigger-etl", ...) remain unchanged) ...
@app.get("/api/upload-url", tags=["Upload"])
async def get_upload_url(filename: str = Query(..., description="Name of the file to upload.")):
    if not api_storage_client: logger_api.error("Cannot generate upload URL: Storage client not available."); raise HTTPException(503, "Storage service unavailable")
    if not filename: raise HTTPException(400, "Filename parameter is required")
    if not API_GCS_BUCKET: raise HTTPException(500, "GCS Bucket configuration missing on server.")
    clean_filename = re.sub(r"[^a-zA-Z0-9_.-]", "_", os.path.basename(filename))
    destination_blob_name = f"uploads/{clean_filename}"
    logger_api.info(f"Generating signed URL for blob: {destination_blob_name} in bucket {API_GCS_BUCKET}")
    try:
        bucket = api_storage_client.bucket(API_GCS_BUCKET); blob = bucket.blob(destination_blob_name)
        url = blob.generate_signed_url(version="v4", expiration=timedelta(minutes=SIGNED_URL_EXPIRATION_MINUTES), method="PUT")
        logger_api.info(f"Signed URL generated successfully for {destination_blob_name}")
        return {"url": url, "object_name": blob.name}
    except NotFound: logger_api.error(f"GCS Bucket not found: {API_GCS_BUCKET}"); raise HTTPException(404, f"GCS Bucket '{API_GCS_BUCKET}' not found.")
    except GoogleAPICallError as e: logger_api.error(f"Google API Call Error generating signed URL for {filename}: {e}", exc_info=True); raise HTTPException(502, f"Error communicating with GCS API: {str(e)}")
    except Exception as e: logger_api.error(f"Error generating signed URL for {filename}: {e}", exc_info=True); raise HTTPException(500, f"Could not generate upload URL: {str(e)}")

@app.post("/api/trigger-etl", tags=["ETL"])
async def trigger_etl(payload: ETLRequest, request: Request):
    if not api_publisher or not api_topic_path: logger_api.error("Cannot trigger ETL: Publisher client not available."); raise HTTPException(503, "Messaging service unavailable")
    if not payload.object_name or not payload.object_name.startswith("uploads/"): logger_api.warning(f"Invalid object_name received for ETL trigger: {payload.object_name}"); raise HTTPException(400, "Invalid object_name provided. Must start with 'uploads/'.")
    client_ip = request.client.host if request.client else "unknown"
    logger_api.info(f"Triggering ETL for object: {payload.object_name} (requested by {client_ip})")
    try:
        data = payload.object_name.encode("utf-8")
        future = api_publisher.publish(api_topic_path, data=data)
        def pubsub_callback(f):
             try: message_id = f.result(); logger_api.info(f"Pub/Sub message published successfully for {payload.object_name}. Message ID: {message_id}")
             except Exception as pub_e: logger_api.error(f"Failed to publish Pub/Sub message for {payload.object_name}: {pub_e}", exc_info=True)
        future.add_done_callback(pubsub_callback)
        logger_api.info(f"ETL job queued successfully for {payload.object_name}")
        return {"status": "queued", "object_name": payload.object_name}
    except GoogleAPICallError as e: logger_api.error(f"Google API Call Error publishing ETL trigger for {payload.object_name} to Pub/Sub: {e}", exc_info=True); raise HTTPException(502, f"Error communicating with Pub/Sub API: {str(e)}")
    except Exception as e: logger_api.error(f"Error publishing ETL trigger for {payload.object_name} to Pub/Sub: {e}", exc_info=True); raise HTTPException(500, f"Could not trigger ETL: {str(e)}")


# --- Include Routers ---
app.include_router(bq_router) # BigQuery endpoints are under /api/bigquery/...

# --- Optional Health Check ---
@app.get("/api/health", tags=["Health"])
async def health_check():
    # ... (health_check remains unchanged) ...
    statuses = {}; overall_status = "ok"
    if api_bigquery_client: statuses["bigquery"] = "ok"
    else: statuses["bigquery"] = "unavailable"; overall_status = "unhealthy"
    if API_GCS_BUCKET:
        if api_storage_client: statuses["storage"] = "ok"
        else: statuses["storage"] = "unavailable"; overall_status = "unhealthy"
    if API_PUBSUB_TOPIC:
        if api_publisher: statuses["pubsub"] = "ok"
        else: statuses["pubsub"] = "unavailable"; overall_status = "unhealthy"
    if overall_status == "unhealthy": raise HTTPException(status_code=503, detail=statuses)
    return {"status": overall_status, "components": statuses}


# --- Root Endpoint ---
@app.get("/", tags=["Root"], include_in_schema=False)
def read_root():
    return {"message": f"{app.title} is running."}

# --- Uvicorn Runner (for local development) ---
if __name__ == "__main__":
    import uvicorn
    api_host = os.getenv("API_HOST", "127.0.0.1")
    api_port = int(os.getenv("API_PORT", 8000))
    logger_api.info(f"Starting Uvicorn server on http://{api_host}:{api_port}")
    uvicorn.run("main:app", host=api_host, port=api_port, reload=True)