# ==============================================================================
# SECTION 1: FastAPI BigQuery Job Runner + Upload/ETL Trigger + AI Features
# ==============================================================================
import os
import logging
import tempfile
import copy
import atexit # To help clean up the temp file
import re
from services.feedback_service import store_feedback
from datetime import timedelta, timezone, datetime, date, time # Added date, time
from typing import List, Dict, Any, Optional, Union
import traceback
import uuid
from routers.chatbot import chat_router
from auth import get_current_user, verify_token
# FastAPI and Pydantic
import base64
from google.cloud.firestore_v1.base_query import FieldFilter # Keep this
from clients import initialize_google_clients, initialize_gemini, _cleanup_temp_cred_file_clients,generate_with_key   # +++ MODIFIED +++
from fastapi import FastAPI, HTTPException, Request, APIRouter, Depends, Query, Path , status,Response,BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse # Keep if used later
from pydantic import BaseModel, Field,conint
import json
from services.etl_status_service import (
    initialize_batch_status_in_firestore,
    update_file_status_in_firestore,
    ETL_BATCHES_COLLECTION # If you need to refer to the collection name directly
)
# Google Cloud Libraries
from google.cloud import bigquery, storage, pubsub_v1
from google.cloud.exceptions import NotFound, BadRequest , Conflict
from google.api_core.exceptions import GoogleAPICallError, DeadlineExceeded
from google.oauth2 import service_account
import google.generativeai as genai
from routers.export import export_router
from services.firestore_service import initialize_firestore,get_firestore_client,get_user_accessible_datasets,get_user_role,register_workspace_and_grant_access,remove_workspace_from_firestore,ensure_user_document_exists   # Import initializer
from dependencies.rbac import require_admin # Import RBAC dependency
from services.firestore_service import db
from routers.user_profile import user_profile_router # Import new router
from config import ( # Import config VARIABLES
    API_GCP_PROJECT, API_GCS_BUCKET, API_PUBSUB_TOPIC, API_CREDENTIALS_PATH,
    DEFAULT_JOB_TIMEOUT_SECONDS, DEFAULT_BQ_LOCATION, SIGNED_URL_EXPIRATION_MINUTES,
    GEMINI_REQUEST_TIMEOUT, GEMINI_API_KEY, ALLOWED_ORIGINS
)
from dependencies.client_deps import (
    get_bigquery_client,
    get_storage_client,
    get_pubsub_publisher,
    get_pubsub_topic_path
)
from pydantic import BaseModel, Field, conint, validator, root_validator
from services.etl_status_service import WorkerFileCompletionPayload
# Utilities
import pandas as pd
from dotenv import load_dotenv
# --- Load Environment Variables ---
load_dotenv()

# --- FastAPI App Initialization ---
app = FastAPI(title="Intelligent BigQuery & ETL API")
bq_router = APIRouter(
    prefix="/api/bigquery",
    tags=["BigQuery"],
    dependencies=[Depends(verify_token)] # Protect all BQ routes
)
feedback_router = APIRouter(
    prefix="/api/feedback",
    tags=["Feedback"],
    dependencies=[Depends(verify_token)] # All feedback submissions require authentication
)
# --- Logging Setup ---
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=log_level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger_api = logging.getLogger("uvicorn.error" if "uvicorn" in os.getenv("SERVER_SOFTWARE", "") else __name__ + "_api")
logger_api.setLevel(log_level)
logger_api.info("FastAPI application starting...")


@app.on_event("startup")
def startup_event():
    logger_api.info("Running startup event: Initializing Gemini and ensuring credentials setup...")
    # Initialize Gemini directly if needed globally
    initialize_gemini()
    initialize_firestore()
    # We still call initialize_google_clients to ensure the temp file
    # and environment variable are set up correctly before the first request.
    # The actual client object creation will be triggered by the first dependency call.
    initialize_google_clients()
    atexit.register(_cleanup_temp_cred_file_clients)
    logger_api.info("Startup pre-initialization complete.")


FEEDBACK_GCS_BUCKET=os.getenv("FEEDBACK_BUCKET")
allowed_origins = os.getenv("ALLOWED_ORIGINS", "*").split(",")
app.add_middleware(CORSMiddleware, allow_origins=allowed_origins, allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
logger_api.info(f"CORS enabled for origins: {allowed_origins}")

# Payload from Frontend to initiate an ETL process for ONE file
class SingleFileETLTriggerClientPayload(BaseModel):
    object_name: str = Field(..., description="GCS object name, including any prefix like 'dataset_prefix/filename.xlsx'")
    target_dataset_id: str = Field(..., description="Target BigQuery dataset (short ID, not project.dataset)")
    original_file_name: str = Field(..., description="The original name of the file uploaded by the user")
    is_multi_header: Optional[bool] = Field(default=False)
    header_depth: Optional[conint(ge=1, le=10)] = Field(None) # ge=1 means min 1 if provided
    apply_ai_smart_cleanup: Optional[bool] = Field(default=False, description="Whether to apply AI-based data value cleaning.") 
    text_normalization_mode: Optional[str] = Field(None, description="Selected AI text normalization mode if smart cleanup is enabled.")
    enable_unpivot: Optional[bool] = Field(default=False)
    unpivot_id_cols_str: Optional[str] = Field(None, description="Comma-separated string of ID columns for unpivot.")
    unpivot_var_name: Optional[str] = Field(None, description="Name for the new attribute/variable column.")
    unpivot_value_name: Optional[str] = Field(None, description="Name for the new value column.")



    @validator('text_normalization_mode', pre=True, always=True)
    def set_text_normalization_mode_if_cleanup_disabled(cls, v, values):
        if 'apply_ai_smart_cleanup' in values and not values['apply_ai_smart_cleanup']:
            return None # Ensure mode is None if cleanup is off
        return v

    @root_validator(pre=False, skip_on_failure=True) # post-validation after individual fields
    def check_unpivot_settings(cls, values):
        enable_unpivot = values.get('enable_unpivot')
        unpivot_id_cols_str = values.get('unpivot_id_cols_str')
        unpivot_var_name = values.get('unpivot_var_name')
        unpivot_value_name = values.get('unpivot_value_name')

        if enable_unpivot:
            if not unpivot_id_cols_str or not unpivot_id_cols_str.strip():
                raise ValueError("If unpivot is enabled, 'Identifier Columns (unpivot_id_cols_str)' must be provided.")
            # Ensure default names if user clears them but unpivot is still enabled
            if not unpivot_var_name or not unpivot_var_name.strip():
                values['unpivot_var_name'] = "Attribute" # Default if empty
            if not unpivot_value_name or not unpivot_value_name.strip():
                values['unpivot_value_name'] = "Value"   # Default if empty
        else:
            # If unpivot is disabled, clear other unpivot fields to avoid sending them
            values['unpivot_id_cols_str'] = None
            values['unpivot_var_name'] = None
            values['unpivot_value_name'] = None
        return values


class FeedbackImageUploadUrlRequest(BaseModel):
    filename: str = Field(..., description="The original name of the image file.")
    content_type: str = Field(..., description="The MIME type of the image (e.g., image/jpeg, image/png).")
    # You could add user_uid here if you want to make the GCS path user-specific,
    # but it's also available from the token in the endpoint.

class FeedbackImageUploadUrlResponse(BaseModel):
    upload_url: str
    gcs_object_name: str # The full path in GCS where the file will be stored
    # You could also return a public_url if your bucket/objects are public and you construct it


# Payload for the Pub/Sub message (sent to the ETL worker)
class ETLRequestPubSubPayload(BaseModel):
    object_name: str
    target_dataset_id: str
    is_multi_header: Optional[bool]
    header_depth: Optional[int]
    batch_id: str             # UUID for the batch this file belongs to
    file_id: str              # UUID for this specific file processing task
    original_file_name: str
    apply_ai_smart_cleanup: Optional[bool] = False # NEW
    text_normalization_mode: Optional[str] = None 
    # +++ NEW UNPIVOT FIELDS +++
    enable_unpivot: Optional[bool] = False
    unpivot_id_cols_list: Optional[List[str]] = None # Worker will expect a list
    unpivot_var_name: Optional[str] = "Attribute"   # Default for worker
    unpivot_value_name: Optional[str] = "Value"     # Default 

# Payload from ETL Worker to report file completion status
class WorkerFileCompletionPayload(BaseModel):
    batch_id: str
    file_id: str
    original_file_name: str # For logging/verification
    success: bool
    error_message: Optional[str] = None
    # Optional: You could add more details like num_tables_processed, bq_table_ids, etc.
# --- Pydantic Models ---
# --- Pydantic Models for Prompt Suggestion ---
class FeedbackSubmission(BaseModel):
    user_prompt: Optional[str] = None
    generated_sql: Optional[str] = None
    dataset_id: Optional[str] = None
    ai_mode: Optional[str] = None
    selected_tables: Optional[List[str]] = None
    selected_columns: Optional[List[str]] = None
    job_id: Optional[str] = None
    job_status_summary: Optional[str] = None # e.g., "SUCCESS", "FAILED", "SUCCESS_BUT_INCORRECT"
    job_error_message: Optional[str] = None
    feedback_type: str # e.g., "Incorrect SQL", "Wrong Results", "Job Error", "Suggestion", "Positive"
    user_description: str = Field(..., min_length=10) # Make description mandatory and have min length
    user_corrected_sql: Optional[str] = None
    page_context: Optional[str] = None # e.g., "/explorer", "/upload"
    image_urls: Optional[List[str]] = None 
    # user_id will be extracted from the token on the backend
    # timestamp will be added by the backend

class FeedbackResponse(BaseModel):
    message: str
    feedback_id: str
class PromptSuggestionRequest(BaseModel):
    current_prompt: str = Field(..., description="The partial prompt typed by the user.")
    # Optional: Add dataset_id if you want suggestions tailored to schema later
    # dataset_id: Optional[str] = None

class PromptSuggestionResponse(BaseModel):
    suggestions: List[str] = Field(default_factory=list, description="List of suggested prompt completions or improvements.")
    error: Optional[str] = None
class TableListItem(BaseModel): tableId: str
class QueryRequest(BaseModel):
    sql: str
    priority: str = Field(default="BATCH", pattern="^(BATCH|INTERACTIVE)$") # Example validation
    use_legacy_sql: bool = False
    default_dataset: str | None = None # Expecting "project.dataset" format
    max_bytes_billed: int | None = None
    location: str | None = None # Make location optional if client might not always know it
class JobSubmitResponse(BaseModel):
    job_id: str
    state: str
    location: str | None # Location might not always be present immediately
class JobStatusResponse(BaseModel): job_id: str; state: str; location: str; statement_type: Optional[str] = None; error_result: Optional[Dict[str, Any]] = None; user_email: Optional[str] = None; creation_time: Optional[datetime] = None; start_time: Optional[datetime] = None; end_time: Optional[datetime] = None; total_bytes_processed: Optional[int] = None; num_dml_affected_rows: Optional[int] = None
class JobResultsResponse(BaseModel): rows: List[Dict[str, Any]]; total_rows_in_result_set: Optional[int] = None; next_page_token: Optional[str] = None; schema_: Optional[List[Dict[str, Any]]] = Field(None, alias="schema")
class TableStatsModel(BaseModel): rowCount: Optional[int] = None; sizeBytes: Optional[int] = None; lastModified: Optional[str] = None
class TableDataResponse(BaseModel): rows: List[Dict[str, Any]]; totalRows: Optional[int] = None; stats: Optional[TableStatsModel] = None
class ETLRequest(BaseModel):
    object_name: str = Field(..., description="Full GCS path of the uploaded object (e.g., dataset_prefix/filename.xlsx)")
    target_dataset_id: str = Field(..., description="The BigQuery dataset ID to load the data into (e.g., my_team_dataset)")
        # +++ NEW FIELDS FOR MULTI-HEADER +++
    is_multi_header: Optional[bool] = Field(default=False, description="Indicates if the file has multi-level headers.")
    header_depth: Optional[conint(ge=1, le=10)] = Field(None, description="Number of rows making up the header, if multi-header. Min 1, Max 10.")
    apply_ai_smart_cleanup: Optional[bool] = Field(default=False, description="Whether to apply AI-based data value cleaning.") 
# +++ MODIFICATION END +++
class ColumnInfo(BaseModel): name: str; type: str; mode: str
class TableSchema(BaseModel): table_id: str; columns: List[ColumnInfo]
class SchemaResponse(BaseModel): dataset_id: str; tables: List[TableSchema]
# +++ MODIFICATION START: Enhance NLQueryRequest +++
class NLQueryRequest(BaseModel):
    prompt: str = Field(...)
    dataset_id: str = Field(..., description="The BigQuery dataset ID to query against.")
    ai_mode: str = Field(default="AUTO", description="AI schema focus mode: AUTO or SEMI_AUTO.")
    selected_tables: Optional[List[str]] = Field(None, description="Tables to focus on in SEMI_AUTO mode.")
    selected_columns: Optional[List[str]] = Field(None, description="Columns to focus on in SEMI_AUTO mode (within selected_tables).")
    # Keep table_prefix if you still want it for AUTO mode or as a fallback
    table_prefix: Optional[str] = Field(None, description="Optional prefix to filter tables shown to the AI (used in AUTO mode or SEMI_AUTO without table selection).")
# +++ MODIFICATION END +++
class NLQueryResponse(BaseModel): generated_sql: Optional[str] = None; error: Optional[str] = None

class UserProfileResponse(BaseModel):
    user_id: str
    role: str
# --- Pydantic Models ---
# ... (keep existing models)

class CreateDatasetRequest(BaseModel):
    dataset_id: str = Field(..., description="The desired ID for the new dataset. Must be unique within the project. Follows BigQuery naming rules (alphanumeric + underscore).", min_length=1, max_length=1024, pattern=r"^[a-zA-Z0-9_]+$")
    location: Optional[str] = Field(DEFAULT_BQ_LOCATION, description="The geographic location for the dataset (e.g., 'US', 'EU', 'asia-northeast1'). Defaults to server config.")
    description: Optional[str] = Field(None, description="A user-friendly description for the dataset.")
    labels: Optional[Dict[str, str]] = Field(None, description="Key-value labels to apply to the dataset.")
    # Add other common options if needed, e.g.:
    # default_table_expiration_ms: Optional[int] = None
    # default_partition_expiration_ms: Optional[int] = None

class DatasetCreatedResponse(BaseModel):
    project_id: str
    dataset_id: str
    location: str
    description: Optional[str] = None
    labels: Optional[Dict[str, str]] = None
    # Add other relevant fields returned by BQ API if needed

class DatasetListItemModel(BaseModel):
    datasetId: str
    location: str
    # You could add other fields like location, project if needed

class DatasetListResponse(BaseModel):
    datasets: List[DatasetListItemModel]

class AISummaryRequest(BaseModel):
    schema_: List[Dict[str, Any]] = Field(..., alias="schema")
    query_sql: str
    original_prompt: Optional[str] = None # The user's natural language question, if available
    result_sample: List[Dict[str, Any]]

class AISummaryResponse(BaseModel):
    summary_text: Optional[str] = None
    error: Optional[str] = None


# --- Helper Functions ---
def serialize_bq_row(row: bigquery.table.Row) -> Dict[str, Any]:
    record = {}
    for key, value in row.items():
        if isinstance(value, bytes):
            try: record[key] = value.decode('utf-8')
            except UnicodeDecodeError: record[key] = f"0x{value.hex()}"
        elif isinstance(value, (datetime, pd.Timestamp)):
            if value.tzinfo is None: value = value.replace(tzinfo=timezone.utc)
            else: value = value.astimezone(timezone.utc)
            record[key] = value.isoformat(timespec='seconds') + 'Z'
        elif isinstance(value, date): record[key] = value.isoformat()
        elif isinstance(value, time): record[key] = value.isoformat()
        elif isinstance(value, list) or isinstance(value, dict): record[key] = value
        else: record[key] = value
    return record

def serialize_bq_schema(schema: List[bigquery.schema.SchemaField]) -> List[Dict[str, Any]]:
    return [{"name": field.name, "type": field.field_type, "mode": field.mode} for field in schema]

# Cache for schema
SCHEMA_CACHE = {}
SCHEMA_CACHE_EXPIRY_SECONDS = 3600 # 1 hour



def get_dataset_schema(dataset_id: str, bq_client: bigquery.Client) -> List[TableSchema]:
    """Fetches schema for all tables in a dataset."""
    # if not api_bigquery_client:
    #     raise HTTPException(status_code=503, detail="BigQuery client not available.")
    # logger_api.info(f"Fetching schema for dataset: {dataset_id}")
    logger_api.info(f"Helper fetching schema for dataset: {dataset_id}")
    table_schemas: List[TableSchema] = []
    try:
        # tables_iterator = api_bigquery_client.list_tables(dataset_id)
        tables_iterator = bq_client.list_tables(dataset_id)
        for tbl_item in tables_iterator:
            try:
                full_table_id = f"{tbl_item.project}.{tbl_item.dataset_id}.{tbl_item.table_id}"
                # table = api_bigquery_client.get_table(full_table_id)
                table = bq_client.get_table(full_table_id)
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
# --- BigQuery API Endpoints (using bq_router) ---




@bq_router.get("/schema", response_model=SchemaResponse)
def get_bigquery_dataset_schema_endpoint( # Renamed function
    dataset_id: str = Query(..., description="Full dataset ID (e.g., project.dataset)"),
    bq_client: bigquery.Client = Depends(get_bigquery_client) # Inject client
):
    """Retrieves the schema (table names, column names, types) for all tables in a dataset."""
    # Consider adding caching here later for performance (e.g., using Redis or in-memory cache)
    try:
        # table_schemas = get_dataset_schema(dataset_id)
        table_schemas = get_dataset_schema(dataset_id, bq_client) # Pass client
        return SchemaResponse(dataset_id=dataset_id, tables=table_schemas)
    except HTTPException as http_exc: raise http_exc
    except Exception as e: logger_api.error(f"Schema endpoint error: {e}", exc_info=True); raise HTTPException(status_code=500, detail=f"Failed schema retrieval: {str(e)}")
    # except HTTPException as http_exc:
    #     # Re-raise HTTP exceptions directly
    #     raise http_exc
    # except Exception as e:
    #     # Catch unexpected errors from helper
    #     logger_api.error(f"Unexpected error in get_dataset_schema endpoint: {e}", exc_info=True)
    #     raise HTTPException(status_code=500, detail=f"Failed to retrieve schema: {str(e)}")




@bq_router.post("/nl2sql", response_model=NLQueryResponse)
def natural_language_to_sql( # Assuming this is part of a class or has app/bq_router defined elsewhere
    req: NLQueryRequest,
    bq_client: Any = Depends(get_bigquery_client) # Replace Any with actual bq_client type
):
    """Converts a natural language prompt into a BigQuery SQL query using Gemini,
    potentially filtered by table prefix or explicit selections."""

    if not GEMINI_API_KEY: # Assuming GEMINI_API_KEY is a global config
        raise HTTPException(status_code=503, detail="NL-to-SQL service not configured.")
    if not req.prompt or req.prompt.isspace():
        raise HTTPException(status_code=400, detail="Prompt cannot be empty.")
    if not req.dataset_id or not req.dataset_id.strip():
        raise HTTPException(status_code=400, detail="Dataset ID cannot be empty.")

    logger_api.info(f"NL-to-SQL request for dataset '{req.dataset_id}'. Mode: {req.ai_mode}. Prompt: '{req.prompt[:100]}...'")
    if req.ai_mode == "SEMI_AUTO":
        logger_api.info(f"  Selected Tables: {req.selected_tables}")
        logger_api.info(f"  Selected Columns: {req.selected_columns}")

    try:
        # 1. Fetch the *full* schema for the dataset first
        all_table_schemas_full = get_dataset_schema(req.dataset_id, bq_client)
        if not all_table_schemas_full:
            # Raise 404 if get_dataset_schema returns empty, implying dataset not found or no tables
            raise HTTPException(status_code=404, detail=f"No tables or schema found for dataset: {req.dataset_id}. Please check the dataset ID or ensure it contains tables.")

        # Use deepcopy to avoid modifying the original fetched schema if it's cached or reused
        all_table_schemas = copy.deepcopy(all_table_schemas_full)

        # 2. Determine the schema to send to the AI based on mode
        schema_to_use: List[TableSchema] = []
        schema_source_description = f"all tables in dataset `{req.dataset_id}`"

        if req.ai_mode == "SEMI_AUTO":
            if not req.selected_tables:
                logger_api.warning("SEMI_AUTO mode selected but no tables provided by the client.")
                raise HTTPException(status_code=400, detail="SEMI_AUTO mode requires at least one table to be selected.")

            selected_tables_set = set(req.selected_tables)
            schema_for_semi_auto = [ts for ts in all_table_schemas if ts.table_id in selected_tables_set]
            
            if not schema_for_semi_auto:
                logger_api.warning(f"SEMI_AUTO: None of the selected tables {req.selected_tables} were found in the schema of dataset {req.dataset_id}.")
                raise HTTPException(status_code=400, detail=f"None of the selected tables were found in the dataset schema. Available tables: {[t.table_id for t in all_table_schemas]}")

            schema_source_description = f"selected tables ({', '.join(sorted(list(selected_tables_set)))})"

            if req.selected_columns:
                filtered_schema_with_cols: List[TableSchema] = []
                selected_columns_set = set(req.selected_columns)
                logger_api.debug(f"SEMI_AUTO: Filtering columns within selected tables against: {selected_columns_set}")

                for ts in schema_for_semi_auto: # Iterate over already table-filtered schema
                    original_columns = ts.columns
                    ts.columns = [c for c in original_columns if c.name in selected_columns_set]
                    if ts.columns: # Keep table only if it still has some of the selected columns
                        filtered_schema_with_cols.append(ts)
                    else:
                        logger_api.info(f"  Table '{ts.table_id}': Removed in SEMI_AUTO as no selected columns remained after filtering.")
                
                schema_to_use = filtered_schema_with_cols
                schema_source_description += f", focusing on columns ({', '.join(sorted(list(selected_columns_set)))})"
            else: # SEMI_AUTO with selected tables but no specific columns (use all columns of selected tables)
                schema_to_use = schema_for_semi_auto
            
            if not schema_to_use: # If after all SEMI_AUTO filtering, schema is empty
                logger_api.warning("SEMI_AUTO mode resulted in an empty schema after all filtering (tables and/or columns).")
                raise HTTPException(status_code=400, detail="The combination of selected tables and columns resulted in an empty schema. Ensure selected columns exist within the selected tables.")
        
        else: # AUTO Mode (or if mode is invalid, default to AUTO behavior)
            schema_to_use = all_table_schemas # Start with all tables
            if req.table_prefix and req.table_prefix.strip():
                 prefix = req.table_prefix.strip()
                 original_count = len(schema_to_use)
                 schema_to_use = [ts for ts in schema_to_use if ts.table_id.startswith(prefix)]
                 schema_source_description = f"tables in dataset `{req.dataset_id}` matching prefix '{prefix}' ({len(schema_to_use)} of {original_count})"
                 logger_api.info(f"AUTO: Filtered schema from {original_count} to {len(schema_to_use)} tables using prefix '{prefix}'.")
                 if not schema_to_use:
                    raise HTTPException(status_code=404, detail=f"No tables found in dataset {req.dataset_id} matching prefix '{prefix}'.")
            # else: schema_source_description remains "all tables in dataset..."

        # 3. Build the schema string for the AI prompt
        schema_lines = []
        for ts in schema_to_use: # Use the filtered schema_to_use
            if ts.columns:
                column_details = []
                for c in ts.columns:
                    col_type_for_ai = c.type.upper()
                    if col_type_for_ai == "INT64": col_type_for_ai = "INTEGER"
                    if col_type_for_ai == "FLOAT64": col_type_for_ai = "FLOAT"
                    column_details.append(f"`{c.name}` ({col_type_for_ai})")
                columns_str = ', '.join(column_details)
                schema_lines.append(f"- Table `{ts.table_id}` (Columns: {columns_str})")

        schema_string = "\n".join(schema_lines).strip()
        full_dataset_id_str = f"`{req.dataset_id}`"

        if not schema_string:
            logger_api.error(f"Schema string became empty after filtering for NL2SQL (Mode: {req.ai_mode}, Source: {schema_source_description}). This implies no qualifying tables/columns were found.")
            # This error message is more informative to the user
            raise HTTPException(status_code=404, detail=f"No relevant table schema could be found to match your request (Mode: {req.ai_mode}). Please check your selections or table prefix.")

        # 4. Construct the Enhanced AI Prompt
        today_for_prompt = datetime.now(timezone.utc).strftime('%Y-%m-%d')

        prompt_template = f"""You are an expert BigQuery SQL generator. Generate a *single*, valid, executable BigQuery SQL query based on the user request and the provided database schema subset.

**Database Schema (Dataset ID: {full_dataset_id_str}):**
The following schema information is derived from: {schema_source_description}.
Each table lists its columns with their data types in parentheses (e.g., `column_name` (TYPE)).
{schema_string}

**User Request:** "{req.prompt}"

**Key Analytical Terms & Functions to Support:**
*   "total", "sum of": Use `SUM()`.
*   "average", "avg", "mean": Use `AVG()`.
*   "count of", "number of": Use `COUNT()`. For distinct items, use `COUNT(DISTINCT ...)`.
*   "distinct count of": Use `COUNT(DISTINCT ...)`.
*   "minimum", "min", "lowest": Use `MIN()`.
*   "maximum", "max", "highest": Use `MAX()`.
*   "median of": Use `PERCENTILE_CONT(column, 0.5) OVER()` or, if grouping is clearly implied by the request, `APPROX_QUANTILES(column, 2)[OFFSET(1)]` within the grouped aggregation. Prefer `PERCENTILE_CONT` if the context suggests a window function over the whole set or partitions.
*   "standard deviation of": Use `STDDEV_POP()`. If "sample standard deviation" is explicitly asked, use `STDDEV_SAMP()`.
*   "variance of": Use `VAR_POP()`. If "sample variance" is asked, use `VAR_SAMP()`.
*   "Xth percentile of" (e.g., "90th percentile of sales"): Use `PERCENTILE_CONT(column, X/100.0) OVER()` or `APPROX_QUANTILES(column, 100)[OFFSET(X)]`.

**Instructions:**
1.  **Query Type:** Generate ONLY a valid BigQuery `SELECT` query. Do not generate DML or DDL.
2.  **STRICT Schema Adherence & Column Ownership (CRITICAL):**
    *   Use ONLY the tables, columns, and data types explicitly listed in the "Database Schema" section above.
    *   DO NOT invent table or column names. DO NOT assume a column exists in a table if it is not listed for that table.
    *   If the user's request requires data not present in the provided schema (e.g., a non-existent column or table), you MUST return ONLY a SQL comment explaining this: `-- Cannot fulfill request: [Specific reason, e.g., 'Column 'user_age' not found in the provided schema.' or 'Information about 'inventory levels' is not available in the tables {', '.join([t.table_id for t in schema_to_use])}.']`
3.  **Joins & Relationships (CRITICAL):**
    *   Analyze the "User Request" and the "Database Schema" to determine necessary table joins.
    *   Join tables using appropriate keys. Pay close attention to data types of join keys; use `CAST` ONLY if necessary and types are compatible for casting (e.g., INTEGER to STRING).
    *   (Your existing specific join path examples for Master_Data tables, if still relevant for the current dataset, should be dynamically injected here if possible, or be general principles the AI learns).
4.  **Single Category from Range Lookups (e.g., Marks to Grade - Your existing detailed instructions):**
    *   (Keep your existing CTE with ROW_NUMBER() or QUALIFY ROW_NUMBER() logic here for this pattern)
5.  **Consolidating Multiple Similar Lookup Tables (Your existing UNION ALL logic):**
    *   (Keep your existing instructions for UNION ALL for similar lookup tables here)
6.  **Mapping User Language:** Carefully map terms from the "User Request" to actual schema elements.
7.  **Qualification:** ALWAYS fully qualify table names (e.g., {full_dataset_id_str}.`YourTableName`). Use table aliases (e.g., `t1`, `t2`) and qualify ALL column names (e.g., `t1.column_name`).
8.  **Syntax:** Use correct BigQuery Standard SQL syntax.
9.  **Assumptions:** If the request is ambiguous or requires an assumption not directly inferable from the schema, AND it prevents query generation, explain in a SQL comment as per instruction 2. Do NOT ask clarifying questions in the SQL output.
10. **Date Handling (VERY IMPORTANT - Today is {today_for_prompt}):**
    *   (Keep your existing detailed date handling instructions: `DATE('YYYY-MM-DD')`, `DATE(timestamp_column)`, `TIMESTAMP('YYYY-MM-DD HH:MM:SS')`, `TIMESTAMP_TRUNC`, and interpreting relative dates using `Today is {today_for_prompt}`.)
11. **Output:** Respond with *only* the raw SQL query text. No explanations, no markdown ```sql ... ``` blocks, unless it's the "Cannot fulfill request" comment.

Generated BigQuery SQL Query:
"""
        logger_api.debug(
            f"Gemini Prompt for NL2SQL (Mode: {req.ai_mode}, Schema Source: {schema_source_description}). "
            f"Prompt text (start): {prompt_template[:500]}..."
        )

        # 5. Call Gemini API
        response_text = generate_with_key( # Assuming generate_with_key is your helper
            1,  # API Key index
            prompt_template,
            GEMINI_REQUEST_TIMEOUT # Assuming GEMINI_REQUEST_TIMEOUT is defined
        )

        # 6. Process Response
        logger_api.debug(f"Gemini Raw Response Text for NL2SQL: {response_text[:500]}...")
        generated_sql = response_text.strip()

        # Cleaning SQL (keep your existing logic, ensure it's robust)
        if generated_sql.startswith("```sql"):
            generated_sql = generated_sql[len("```sql"):].strip()
        if generated_sql.endswith("```"):
            generated_sql = generated_sql[:-len("```")].strip()
        
        # More careful comment removal: remove only if it's a standalone comment line
        # or if the entire response is just comments.
        # For now, let's assume a primary SQL block is expected.
        # If the response IS a "-- Cannot fulfill request..." comment, we want to keep it.
        cannot_fulfill_match = re.match(r"^\s*-- Cannot fulfill request: (.*)", generated_sql, re.IGNORECASE | re.DOTALL)
        
        if cannot_fulfill_match:
            reason = cannot_fulfill_match.group(1).strip()
            logger_api.warning(f"Gemini indicated request cannot be fulfilled for NL2SQL: {reason}")
            return NLQueryResponse(error=f"AI could not generate query: {reason}") # Return the specific reason
        else:
            # If not a "cannot fulfill" message, remove other leading/trailing comment blocks
            # This is tricky; a robust way might be to find the first SELECT/WITH
            # For now, let's use a slightly less aggressive removal
            lines = generated_sql.splitlines()
            sql_lines = [line for line in lines if not line.strip().startswith("--")] # Keep lines not starting with --
            generated_sql = "\n".join(sql_lines).strip()


        if not generated_sql:
            logger_api.warning("Gemini returned an empty response or only comments after cleaning for NL2SQL.")
            return NLQueryResponse(error="AI failed to generate a query. Please try rephrasing or check the AI service status.")
        
        # Check if it starts with SELECT or WITH (for CTEs)
        if not (generated_sql.lower().lstrip().startswith("select") or generated_sql.lower().lstrip().startswith("with")):
            logger_api.warning(f"Generated text for NL2SQL does not start with SELECT or WITH: {generated_sql[:100]}...")
            # You might decide to return this as an error or let BQ attempt to run it
            # For stricter validation:
            # return NLQueryResponse(error=f"AI did not generate a valid SELECT or WITH query. Output preview: {generated_sql[:50]}...")


        logger_api.info(f"NL-to-SQL successful. Generated SQL (start): {generated_sql[:100]}...")
        return NLQueryResponse(generated_sql=generated_sql)

    except HTTPException as http_exc:
        # Re-raise HTTPExceptions (like 400s, 404s from schema/input validation) directly
        raise http_exc
    except Exception as e: # Catch other errors like DeadlineExceeded, GoogleAPICallError from generate_with_key
        # Log the full error for debugging
        logger_api.error(f"Error during NL-to-SQL generation for dataset '{req.dataset_id}', prompt '{req.prompt[:50]}': {e}", exc_info=True)
        # Provide a generic error to the user, but specific enough if it's a timeout or API issue
        if "DeadlineExceeded" in str(type(e)): # Crude check, better to catch specific exception if generate_with_key raises it
            raise HTTPException(status_code=504, detail="NL-to-SQL generation timed out. Please try a simpler prompt or try again later.")
        elif "GoogleAPICallError" in str(type(e)): # Crude check
             raise HTTPException(status_code=502, detail=f"Error communicating with AI service for NL-to-SQL: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to generate SQL due to an unexpected server error: {str(e)}")




# --- Other Endpoints ---
# (Keep /jobs, /tables, /table-data, /jobs/{job_id}, /jobs/{job_id}/results, /api/upload-url, /api/trigger-etl, /api/health, / EXACTLY as they were)
@bq_router.post("/jobs", response_model=JobSubmitResponse, status_code=202)
def submit_bigquery_job(
    req: QueryRequest,
    bq_client: bigquery.Client = Depends(get_bigquery_client) # Inject client
):
    """Submits a BigQuery job."""
    if not req.sql or req.sql.isspace(): raise HTTPException(400, "SQL query required.")
    logger_api.info(f"Submitting job: {req.sql[:100]}...")
    job_config = bigquery.QueryJobConfig(priority=req.priority.upper(), use_legacy_sql=req.use_legacy_sql)
    if req.default_dataset: 
        try: p, d = req.default_dataset.split('.'); job_config.default_dataset = bigquery.DatasetReference(p, d) 
        except ValueError: raise HTTPException(400,"Invalid default_dataset format.")
    if req.max_bytes_billed is not None: job_config.maximum_bytes_billed = req.max_bytes_billed
    try:
        query_job = bq_client.query(req.sql, job_config=job_config, location=req.location or DEFAULT_BQ_LOCATION) # Use injected client
        return JobSubmitResponse(job_id=query_job.job_id, state=query_job.state, location=query_job.location)
    except BadRequest as e: logger_api.error(f"Invalid Query: {e}"); raise HTTPException(400, f"Invalid query: {str(e)}")
    except GoogleAPICallError as e: logger_api.error(f"BQ API Error: {e}", exc_info=True); raise HTTPException(502, f"BQ API Error: {str(e)}")
    except Exception as e: logger_api.error(f"Job submission error: {e}", exc_info=True); raise HTTPException(500, f"Unexpected error: {str(e)}")





@bq_router.get("/tables", response_model=List[TableListItem])
def list_bigquery_tables(
    dataset_id: str = Query(..., description="Full dataset ID"),
    bq_client: bigquery.Client = Depends(get_bigquery_client) # Inject client
):
    # if not api_bigquery_client: raise HTTPException(503, "BigQuery client not available.")
    if not dataset_id: raise HTTPException(400, "dataset_id query parameter is required")
    logger_api.info(f"Listing tables for dataset: {dataset_id}")
    try:
        # tables_iterator = api_bigquery_client.list_tables(dataset_id)
        tables_iterator = bq_client.list_tables(dataset_id) # Use injected client
        results = [TableListItem(tableId=table.table_id) for table in tables_iterator]
        # logger_api.info(f"Found {len(results)} tables in dataset {dataset_id}")
        return results
    except NotFound: logger_api.warning(f"Dataset not found: {dataset_id}"); raise HTTPException(404, f"Dataset not found: {dataset_id}")
    except GoogleAPICallError as e: logger_api.error(f"BQ API Error listing tables: {e}"); raise HTTPException(502, f"Error communicating with BQ API: {str(e)}")
    except Exception as e: logger_api.error(f"Error listing tables: {e}"); raise HTTPException(500, f"Unexpected error: {str(e)}")
    # except NotFound: logger_api.warning(f"Dataset not found during table list: {dataset_id}"); raise HTTPException(404, f"Dataset not found: {dataset_id}")
    # except GoogleAPICallError as e: logger_api.error(f"Google API Call Error listing tables for {dataset_id}: {e}", exc_info=True); raise HTTPException(502, f"Error communicating with BigQuery API: {str(e)}")
    # except Exception as e: logger_api.error(f"Unexpected error listing tables for {dataset_id}: {e}", exc_info=True); raise HTTPException(500, f"An unexpected error occurred while listing tables: {str(e)}")



@bq_router.get("/table-data", response_model=TableDataResponse)
def get_table_data(
    dataset_id: str = Query(..., description="Full dataset ID"),
    table_id: str = Query(..., description="Table name"),
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
    bq_client: bigquery.Client = Depends(get_bigquery_client) # Inject client
):
    # if not api_bigquery_client: raise HTTPException(503, "BigQuery client not available.")
    full_table_id = f"{dataset_id}.{table_id}"
    # logger_api.info(f"Fetching preview data for table: {full_table_id}, page: {page}, limit: {limit}")
    logger_api.info(f"Fetching preview data: {full_table_id}, page: {page}, limit: {limit}")
    try:
        # table_ref = api_bigquery_client.get_table(full_table_id)
        table_ref = bq_client.get_table(full_table_id) # Use injected client
        offset = (page - 1) * limit
        # rows_iterator = api_bigquery_client.list_rows(table_ref, start_index=offset, max_results=limit, timeout=60)
        rows_iterator = bq_client.list_rows(table_ref, start_index=offset, max_results=limit, timeout=60) # Use injected client
        results = [serialize_bq_row(row) for row in rows_iterator]
        total_rows = table_ref.num_rows
        stats = TableStatsModel(rowCount=table_ref.num_rows, sizeBytes=table_ref.num_bytes, lastModified=table_ref.modified.isoformat() if table_ref.modified else None)
        return TableDataResponse(rows=results, totalRows=table_ref.num_rows, stats=stats)
    except NotFound: logger_api.warning(f"Table not found: {full_table_id}"); raise HTTPException(404, f"Table not found: {full_table_id}")
    except GoogleAPICallError as e: logger_api.error(f"BQ API Error fetching preview: {e}"); raise HTTPException(502, f"BQ API Error: {str(e)}")
    except Exception as e: logger_api.error(f"Error fetching preview: {e}"); raise HTTPException(500, f"Unexpected table preview error: {str(e)}")
#         logger_api.info(f"Returning {len(results)} preview rows for {full_table_id} (page {page}), total rows: {total_rows}")
#         return TableDataResponse(rows=results, totalRows=total_rows, stats=stats)
#     except NotFound: logger_api.warning(f"Table not found during preview fetch: {full_table_id}"); raise HTTPException(404, f"Table not found: {full_table_id}")
#     except GoogleAPICallError as e: logger_api.error(f"Google API Call Error fetching preview data for {full_table_id}: {e}", exc_info=True); raise HTTPException(502, f"Error communicating with BigQuery API: {str(e)}")
#     except Exception as e: logger_api.error(f"Unexpected error fetching preview data for {full_table_id}: {e}", exc_info=True); raise HTTPException(500, f"An unexpected error occurred while fetching table preview: {str(e)}")

# @app.on_event("startup")
# def startup_event():
#     initialize_firestore()

@bq_router.get("/jobs/{job_id}", response_model=JobStatusResponse)
def get_bigquery_job_status(
    job_id: str = Path(...),
    location: str = Query(DEFAULT_BQ_LOCATION),
    bq_client: bigquery.Client = Depends(get_bigquery_client) # Inject client
):
    # if not api_bigquery_client: raise HTTPException(503, "BigQuery client not available.")
    # logger_api.debug(f"Fetching status for Job ID: {job_id}, Location: {location}")
    """Gets the status of a specific BigQuery job."""
    logger_api.debug(f"Fetching status Job ID: {job_id}, Location: {location}")
    try:
        job = bq_client.get_job(job_id, location=location) # Use injected client
        error_detail = job.error_result if job.error_result else None
        # Manually map fields or ensure JobStatusResponse matches job properties
        return JobStatusResponse(
            job_id=job.job_id, state=job.state, location=job.location,
            statement_type=job.statement_type, error_result=error_detail,
            user_email=job.user_email, creation_time=job.created,
            start_time=job.started, end_time=job.ended,
            total_bytes_processed=job.total_bytes_processed,
            num_dml_affected_rows=getattr(job, 'num_dml_affected_rows', None)
        )
    except NotFound: logger_api.warning(f"Job not found: {job_id}"); raise HTTPException(404, f"Job '{job_id}' not found.")
    except GoogleAPICallError as e: logger_api.error(f"BQ API Error fetching status: {e}"); raise HTTPException(502, f"BQ API Error: {str(e)}")
    except Exception as e: logger_api.error(f"Error fetching status: {e}"); raise HTTPException(500, f"Unexpected job status error: {str(e)}")
    #     job = api_bigquery_client.get_job(job_id, location=location)
    #     error_detail = None
    #     if job.error_result: error_detail = {"reason": job.error_result.get("reason"), "location": job.error_result.get("location"), "message": job.error_result.get("message")}
    #     return JobStatusResponse(job_id=job.job_id, state=job.state, location=job.location, statement_type=job.statement_type, error_result=error_detail, user_email=job.user_email, creation_time=job.created, start_time=job.started, end_time=job.ended, total_bytes_processed=job.total_bytes_processed, num_dml_affected_rows=getattr(job, 'num_dml_affected_rows', None))
    # except NotFound: logger_api.warning(f"Job not found: {job_id} in location {location}"); raise HTTPException(404, f"Job '{job_id}' not found in location '{location}'.")
    # except GoogleAPICallError as e: logger_api.error(f"Google API Call Error fetching job status: {e}", exc_info=True); raise HTTPException(502, f"Error communicating with BigQuery API: {str(e)}")
    # except Exception as e: logger_api.error(f"Unexpected error fetching job status for {job_id}: {e}", exc_info=True); raise HTTPException(500, f"An unexpected error occurred: {str(e)}")




@bq_router.get("/jobs/{job_id}/results", response_model=JobResultsResponse)
def get_bigquery_job_results(
    job_id: str = Path(...),
    location: str = Query(DEFAULT_BQ_LOCATION),
    page_token: Optional[str] = Query(None),
    max_results: int = Query(100, ge=1, le=1000),
    bq_client: bigquery.Client = Depends(get_bigquery_client) # Inject client
):
    """Gets the results of a completed BigQuery job."""
    logger_api.debug(f"Fetching results Job ID: {job_id}, Location: {location}")
    # if not api_bigquery_client: raise HTTPException(503, "BigQuery client not available.")
    # logger_api.debug(f"Fetching results for Job ID: {job_id}, Location: {location}, PageToken: {page_token}, MaxResults: {max_results}")
    try:
        job = bq_client.get_job(job_id, location=location) # Use injected client
        if job.state != 'DONE': raise HTTPException(400, f"Job not complete: {job.state}")
        if job.error_result: err_msg = job.error_result.get('message', 'Unknown'); raise HTTPException(400, f"Job failed: {err_msg}")
        if not job.destination: logger_api.info(f"Job {job_id} had no dest table."); affected_rows = getattr(job, 'num_dml_affected_rows', 0); return JobResultsResponse(rows=[], total_rows_in_result_set=affected_rows or 0, schema=[])
        rows_iterator = bq_client.list_rows(job.destination, max_results=max_results, page_token=page_token, timeout=DEFAULT_JOB_TIMEOUT_SECONDS) # Use injected client
        # job = api_bigquery_client.get_job(job_id, location=location)
        # if job.state != 'DONE': raise HTTPException(400, f"Job {job_id} is not complete. Current state: {job.state}")
        # if job.error_result: error_msg = job.error_result.get('message', 'Unknown error'); raise HTTPException(400, f"Job {job_id} failed: {error_msg}")
        # if not job.destination: logger_api.info(f"Job {job_id} completed but did not produce a destination table."); affected_rows = getattr(job, 'num_dml_affected_rows', 0); return JobResultsResponse(rows=[], total_rows_in_result_set=affected_rows if affected_rows is not None else 0, schema=[])
        # rows_iterator = api_bigquery_client.list_rows(job.destination, max_results=max_results, page_token=page_token, timeout=DEFAULT_JOB_TIMEOUT_SECONDS)
        serialized_rows = [serialize_bq_row(row) for row in rows_iterator]
        schema_info = serialize_bq_schema(rows_iterator.schema)
        return JobResultsResponse(rows=serialized_rows, total_rows_in_result_set=rows_iterator.total_rows, next_page_token=rows_iterator.next_page_token, schema=schema_info)
    except NotFound: logger_api.warning(f"Job/results not found: {job_id}"); raise HTTPException(404, f"Job '{job_id}' results not found.")
    except GoogleAPICallError as e: logger_api.error(f"BQ API Error fetching results: {e}"); raise HTTPException(502, f"BQ API Error: {str(e)}")
    except Exception as e: logger_api.error(f"Error fetching results: {e}"); raise HTTPException(500, f"Unexpected job results error: {str(e)}")
    # except NotFound: logger_api.warning(f"Job or destination table not found for job {job_id} in location {location}"); raise HTTPException(404, f"Job '{job_id}' or its results not found in location '{location}'.")
    # except GoogleAPICallError as e: logger_api.error(f"Google API Call Error fetching job results: {e}", exc_info=True); raise HTTPException(502, f"Error communicating with BigQuery API: {str(e)}")
    # except Exception as e: logger_api.error(f"Unexpected error fetching job results for {job_id}: {e}", exc_info=True); raise HTTPException(500, f"An unexpected error occurred: {str(e)}")



# --- Upload/ETL Endpoints ---
@app.get("/api/upload-url", tags=["Upload"], dependencies=[Depends(verify_token)])
def get_upload_url(
    filename: str = Query(..., description="Name of the file to upload."),
    dataset_id: str = Query(..., description="Target dataset ID"),
    storage_client: storage.Client = Depends(get_storage_client) # Inject storage client
):
    """Generates a GCS signed URL for uploading a file to a specific dataset's prefix."""
    # if not api_storage_client: logger_api.error("Cannot generate upload URL: Storage client not available."); raise HTTPException(503, "Storage service unavailable")
    # if not filename: raise HTTPException(400, "Filename parameter is required")
    # if not dataset_id: raise HTTPException(400, "dataset_id parameter is required") # Validate dataset_id
    # if not API_GCS_BUCKET: raise HTTPException(500, "GCS Bucket configuration missing on server.") # Check for the managed bucket
    if not filename or not dataset_id: raise HTTPException(400, "Filename and dataset_id required")
    if not API_GCS_BUCKET: raise HTTPException(500, "GCS Bucket config missing.")
    # Sanitize filename and dataset_id for path safety (basic example)
    clean_filename = re.sub(r"[^a-zA-Z0-9_.-]", "_", os.path.basename(filename))
    # IMPORTANT: Implement proper sanitization/validation for dataset_id if it comes directly from user input
    # For now, assume it's a valid BQ dataset ID format used as a prefix.
    safe_dataset_prefix = re.sub(r"[^a-zA-Z0-9_]", "_", dataset_id) # Basic sanitization

    # Construct the blob path using the dataset prefix
    destination_blob_name = f"{safe_dataset_prefix}/{clean_filename}" # Path: dataset_id/fMilliame

    logger_api.info(f"Generating signed URL for blob: {destination_blob_name} in MANAGED bucket {API_GCS_BUCKET} (Target Dataset: {dataset_id})")
    try:
        bucket = storage_client.bucket(API_GCS_BUCKET) # Use injected client
        blob = bucket.blob(destination_blob_name)
        url = blob.generate_signed_url(version="v4", expiration=timedelta(minutes=SIGNED_URL_EXPIRATION_MINUTES), method="PUT")
        logger_api.info(f"Generated signed URL for {destination_blob_name}")
        return {"url": url, "object_name": blob.name, "target_dataset_id": dataset_id}
        # bucket = api_storage_client.bucket(API_GCS_BUCKET); blob = bucket.blob(destination_blob_name)
        # url = blob.generate_signed_url(version="v4", expiration=timedelta(minutes=SIGNED_URL_EXPIRATION_MINUTES), method="PUT")
        # logger_api.info(f"Signed URL generated successfully for {destination_blob_name}")
        # # Return the full object name (including prefix) and the target dataset
        # return {"url": url, "object_name": blob.name, "target_dataset_id": dataset_id}
    except NotFound: logger_api.error(f"GCS Bucket not found: {API_GCS_BUCKET}"); raise HTTPException(404, f"GCS Bucket '{API_GCS_BUCKET}' not found.")
    except GoogleAPICallError as e: logger_api.error(f"GCS API Error signed URL: {e}"); raise HTTPException(502, f"GCS API Error: {str(e)}")
    except Exception as e: logger_api.error(f"Signed URL error: {e}"); raise HTTPException(500, f"Unexpected signed URL error: {str(e)}")
    # except NotFound: logger_api.error(f"GCS Bucket not found: {API_GCS_BUCKET}"); raise HTTPException(404, f"GCS Bucket '{API_GCS_BUCKET}' not found.")
    # except GoogleAPICallError as e: logger_api.error(f"Google API Call Error generating signed URL for {filename} (dataset: {dataset_id}): {e}", exc_info=True); raise HTTPException(502, f"Error communicating with GCS API: {str(e)}")





@app.post("/api/trigger-etl", response_model=Dict[str, Any], tags=["ETL"], dependencies=[Depends(verify_token)])
def trigger_etl(
    payload: SingleFileETLTriggerClientPayload, # Uses updated model
    request: Request,                           # FastAPI provided
    background_tasks: BackgroundTasks,          # FastAPI provided
    current_user: dict = Depends(verify_token),
    publisher: pubsub_v1.PublisherClient = Depends(get_pubsub_publisher),
    topic_path: str = Depends(get_pubsub_topic_path)
):
    """
    Triggers the ETL process for a single file, initializes batch tracking in Firestore,
    and publishes a message to Pub/Sub for the worker.
    """
    user_uid = current_user.get("uid")
    if not user_uid:
        logger_api.error("User not authenticated in trigger_etl.")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not authenticated.")

    # --- Basic Payload Sanity Checks (already in your provided code) ---
    if not payload.object_name or not payload.target_dataset_id:
        # This should ideally be caught by Pydantic if fields are not Optional and have no default
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Object name and target dataset ID are required.")
    if payload.is_multi_header and payload.header_depth is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="header_depth is required when is_multi_header is true.")
    if not payload.is_multi_header and payload.header_depth is not None:
        logger_api.warning(
            f"header_depth provided for GCS object {payload.object_name} "
            f"by user {user_uid}, but is_multi_header is false. header_depth will be ignored by worker."
        )
        # Pydantic model or logic here can decide if payload.header_depth should be set to None

    client_ip = request.client.host if request.client else "unknown_ip"
    logger_api.info(
        f"User {user_uid} ({client_ip}) triggering ETL for: {payload.original_file_name} -> GCS: {payload.object_name} "
        f"MultiHeader: {payload.is_multi_header}, Depth: {payload.header_depth}, "
        f"AICleanup: {payload.apply_ai_smart_cleanup}, TextMode: {payload.text_normalization_mode}, "
        f"EnableUnpivot: {payload.enable_unpivot}, UnpivotIDs: '{payload.unpivot_id_cols_str}', "
        f"UnpivotVar: '{payload.unpivot_var_name}', UnpivotVal: '{payload.unpivot_value_name}'"
    )

    # --- Prepare file detail for Firestore batch initialization ---
    # (This part includes the new unpivot fields from the payload)
    file_detail_for_batch_init = {
        "original_file_name": payload.original_file_name,
        "gcs_object_name": payload.object_name,
        "is_multi_header": payload.is_multi_header,
        "header_depth": payload.header_depth,
        "apply_ai_smart_cleanup": payload.apply_ai_smart_cleanup,
        "text_normalization_mode": payload.text_normalization_mode,
        # +++ Include unpivot settings for Firestore logging/tracking if desired +++
        "enable_unpivot": payload.enable_unpivot,
        "unpivot_id_cols_str": payload.unpivot_id_cols_str, # Log the string version
        "unpivot_var_name": payload.unpivot_var_name,
        "unpivot_value_name": payload.unpivot_value_name,
    }

    batch_init_response = initialize_batch_status_in_firestore(user_uid, [file_detail_for_batch_init])

    if not batch_init_response or "batch_id" not in batch_init_response or "file_ids_map" not in batch_init_response:
        logger_api.error(f"Failed to initialize Firestore batch tracking for user {user_uid}, file {payload.original_file_name}.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to initialize ETL batch tracking.")

    batch_id = batch_init_response["batch_id"]
    file_id = batch_init_response["file_ids_map"].get(payload.original_file_name)

    if not file_id: 
        logger_api.error(f"Critical error: File ID not found for {payload.original_file_name} in batch {batch_id} after Firestore initialization.")
        try:
            get_firestore_client().collection(ETL_BATCHES_COLLECTION).document(batch_id).update({
                "overallBatchStatus": "error_internal",
                "internalErrorMessage": "Failed to retrieve file_id post-initialization."
            })
        except Exception as fs_clean_err:
            logger_api.error(f"Failed to mark batch {batch_id} as errored after file_id retrieval failure: {fs_clean_err}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal error creating file tracking ID.")

    # --- Prepare Pub/Sub Message ---
    unpivot_id_cols_list_for_worker: Optional[List[str]] = None
    if payload.enable_unpivot and payload.unpivot_id_cols_str: # unpivot_id_cols_str validated by Pydantic
        unpivot_id_cols_list_for_worker = [col.strip() for col in payload.unpivot_id_cols_str.split(',') if col.strip()]
        if not unpivot_id_cols_list_for_worker:
            # This case means the string was all commas/whitespace, or became empty after Pydantic's processing.
            # The Pydantic validator should ideally prevent this if unpivot_id_cols_str is mandatory when enable_unpivot is true.
            # If it still gets here, we might disable unpivot for the worker or raise error.
            logger_api.warning(f"Unpivot was enabled for '{payload.original_file_name}' but ID columns list is effectively empty. Disabling unpivot for worker.")
            payload.enable_unpivot = False # Override for safety

    # Ensure var_name and value_name have defaults for PubSub payload if unpivot is enabled
    # The Pydantic model `ETLRequestPubSubPayload` already has defaults, but we can be explicit.
    var_name_for_worker = payload.unpivot_var_name if payload.enable_unpivot and payload.unpivot_var_name and payload.unpivot_var_name.strip() else "Attribute"
    value_name_for_worker = payload.unpivot_value_name if payload.enable_unpivot and payload.unpivot_value_name and payload.unpivot_value_name.strip() else "Value"

    pubsub_message_payload = ETLRequestPubSubPayload(
        object_name=payload.object_name,
        target_dataset_id=payload.target_dataset_id,
        is_multi_header=payload.is_multi_header,
        header_depth=payload.header_depth,
        batch_id=batch_id,
        file_id=file_id,
        original_file_name=payload.original_file_name,
        apply_ai_smart_cleanup=payload.apply_ai_smart_cleanup,
        text_normalization_mode=payload.text_normalization_mode if payload.apply_ai_smart_cleanup else None,
        # +++ Set unpivot fields for Pub/Sub message +++
        enable_unpivot=payload.enable_unpivot, # Use potentially overridden value
        unpivot_id_cols_list=unpivot_id_cols_list_for_worker if payload.enable_unpivot else None,
        unpivot_var_name=var_name_for_worker, # Use determined name
        unpivot_value_name=value_name_for_worker  # Use determined name
    )
    message_data_dict = pubsub_message_payload.model_dump() # For Pydantic v2, or .dict() for v1
    data_bytes = json.dumps(message_data_dict).encode("utf-8")
    logger_api.info(f"Attempting to publish to Pub/Sub. Full payload being serialized: {message_data_dict}")
    
    try:
        future = publisher.publish(topic_path, data=data_bytes)
        logger_api.debug(f"Pub/Sub message publish initiated for batch {batch_id}, file {file_id}.")
        
        def pubsub_result_callback(f): # Keep f as is for future's result
            try:
                message_id_published = f.result() 
                logger_api.info(
                    f"Pub/Sub message published successfully for Batch: {batch_id}, FileID: {file_id}, "
                    f"OriginalName: {payload.original_file_name}. Pub/Sub Message ID: {message_id_published}. "
                    f"Payload: {message_data_dict}" # Log the actual sent payload
                )
            except Exception as pub_e_fail:
                logger_api.error(f"Pub/Sub publish FAILED for Batch: {batch_id}, FileID: {file_id}: {pub_e_fail}", exc_info=True)
                # Prepare payload for Firestore update on failure
                error_payload_for_firestore = WorkerFileCompletionPayload(
                    batch_id=batch_id,
                    file_id=file_id,
                    original_file_name=payload.original_file_name,
                    success=False,
                    error_message="Failed to queue for processing (Pub/Sub publish error)."
                )
                background_tasks.add_task(update_file_status_in_firestore, error_payload_for_firestore)

        future.add_done_callback(pubsub_result_callback)

        # Update Firestore status to indicate it's been sent to the worker queue
        get_firestore_client().collection(ETL_BATCHES_COLLECTION).document(batch_id).update({
             f"files.{file_id}.status": "triggered_to_worker",
             f"files.{file_id}.lastUpdated": datetime.now(timezone.utc).isoformat()
         })

        logger_api.info(f"ETL job triggered and Firestore status updated for {payload.original_file_name} (Batch: {batch_id}, File: {file_id})")
        return {"status": "queued", "object_name": payload.object_name, "batch_id": batch_id, "file_id": file_id}

    except Exception as e: 
        logger_api.error(f"Error during Pub/Sub publish or Firestore update for {payload.original_file_name} (Batch {batch_id}): {e}", exc_info=True)
        if batch_id and file_id: # Ensure we have IDs to report failure against
            error_payload = WorkerFileCompletionPayload(
                batch_id=batch_id,
                file_id=file_id, 
                original_file_name=payload.original_file_name,
                success=False,
                error_message=f"API error during trigger: {str(e)}"
            )
            background_tasks.add_task(update_file_status_in_firestore, error_payload)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Could not fully trigger ETL processing: {str(e)}")





# --- Optional Health Check ---
@app.get("/api/health", tags=["Health"])
def health_check( # Inject clients to check their status
    bq_client: Optional[bigquery.Client] = Depends(get_bigquery_client, use_cache=False),
    storage_client: Optional[storage.Client] = Depends(get_storage_client, use_cache=False),
    publisher: Optional[pubsub_v1.PublisherClient] = Depends(get_pubsub_publisher, use_cache=False)
):
    """Checks the status of backend components and dependencies."""
    statuses = {}
    statuses["bigquery"] = "ok" if bq_client else "unavailable"
    statuses["storage"] = "ok" if storage_client else "unavailable"
    statuses["pubsub"] = "ok" if publisher else "unavailable"
    statuses["gemini"] = "ok" if GEMINI_API_KEY else "unavailable"
    overall_status = "ok" if all(s == "ok" for s in statuses.values()) else "unhealthy"
    if overall_status == "unhealthy":
        # Return 503 only if critical components fail
        if statuses["bigquery"] == "unavailable" or statuses["firebase_auth"] == "uninitialized":
             raise HTTPException(status_code=503, detail=statuses)
        else: # Return 200 but indicate unhealthy state for non-critical issues
             return {"status": overall_status, "components": statuses}
    return {"status": overall_status, "components": statuses}
    # if api_bigquery_client: statuses["bigquery"] = "ok"
    # else: statuses["bigquery"] = "unavailable"; overall_status = "unhealthy"
    # if API_GCS_BUCKET:
    #     if api_storage_client: statuses["storage"] = "ok"
    #     else: statuses["storage"] = "unavailable"; overall_status = "unhealthy"
    # if API_PUBSUB_TOPIC:
    #     if api_publisher: statuses["pubsub"] = "ok"
    #     else: statuses["pubsub"] = "unavailable"; overall_status = "unhealthy"
    # if overall_status == "unhealthy": raise HTTPException(status_code=503, detail=statuses)
    # return {"status": overall_status, "components": statuses}

# --- Root Endpoint ---
@app.get("/", tags=["Root"], include_in_schema=False)
def read_root():
    return {"message": f"{app.title} is running."}

# main.py (or your FastAPI routes file)

# --- Add these Pydantic models if they aren't already there ---
class VizSuggestion(BaseModel):
    chart_type: str # e.g., "bar", "line", "scatter", "pie"
    x_axis_column: str
    y_axis_columns: List[str]
    rationale: Optional[str] = None

class SuggestVizRequest(BaseModel):
    schema_: List[Dict[str, Any]] = Field(..., alias="schema")
    query_sql: Optional[str] = None
    prompt: Optional[str] = None
    result_sample: Optional[List[Dict[str, Any]]] = None

class SuggestVizResponse(BaseModel):
    suggestions: List[VizSuggestion]
    error: Optional[str] = None

# +++ NEW ENDPOINT: /summarize-results +++
@bq_router.post("/summarize-results", response_model=AISummaryResponse)
def summarize_results(req: AISummaryRequest):
    """Generates a natural language summary of query results using Gemini."""
    if not GEMINI_API_KEY:
        logger_api.warning("AI Summary requested but Gemini API key not set.")
        return AISummaryResponse(error="AI summary service not configured.")

    if not req.schema_:
        raise HTTPException(status_code=400, detail="Schema information is required for summary.")
    if not req.result_sample:
         raise HTTPException(status_code=400, detail="Result sample is required for summary.")
    if not req.query_sql:
         raise HTTPException(status_code=400, detail="SQL query is required for context.")

    logger_api.info(f"Received AI summary request. SQL: {req.query_sql[:50]}..., Prompt: {req.original_prompt[:50] if req.original_prompt else 'N/A'}")

    try:
        # Prepare schema string
        schema_desc = "\n".join([f"- Column '{s.get('name', 'Unknown')}' (Type: {s.get('type', '?')}, Mode: {s.get('mode', 'NULLABLE')})" for s in req.schema_])

        # Prepare sample string (limited rows for prompt length)
        sample_str = ""
        # Limit sample size further if needed, e.g., max 10 rows, 500 chars total?
        MAX_SAMPLE_ROWS_FOR_SUMMARY = 10
        sample_to_send = req.result_sample[:MAX_SAMPLE_ROWS_FOR_SUMMARY]

        if sample_to_send:
            try:
                # Use JSON representation for clarity in the prompt
                sample_str = f"\nResult Sample (up to {MAX_SAMPLE_ROWS_FOR_SUMMARY} rows):\n```json\n"
                sample_str += json.dumps(sample_to_send, indent=2, default=str) # Use default=str for non-serializable types
                sample_str += "\n```\n"
            except Exception as e_sample:
                logger_api.warning(f"Could not format result sample as JSON for summary prompt: {e_sample}")
                sample_str = "\n(Could not process sample data for prompt)\n"

        # Construct context parts
        prompt_context_str = f"The user executed the following SQL query:\n```sql\n{req.query_sql}\n```"
        if req.original_prompt:
            prompt_context_str += f"\n\nThis query was likely generated from the user's original request: \"{req.original_prompt}\""

        # --- Define the Summary Prompt ---
        summary_prompt = f"""You are a data analyst assistant. A user ran a query and obtained results. Your task is to provide a concise, insightful summary of the key findings based on the provided context and data sample.

Context:
{prompt_context_str}

Result Schema:
{schema_desc}

{sample_str}

Instructions:
1.  **Analyze the Goal:** Consider the original user request (if provided) and the executed SQL query to understand what the user was trying to find.
2.  **Interpret the Sample:** Examine the sample data rows. Identify key patterns, trends, maximums, minimums, or notable distributions relevant to the user's goal.
3.  **Synthesize Findings:** Write a brief (2-4 sentences) natural language summary focusing on the most important insights derived from the data sample in relation to the query's purpose.
4.  **Actionable Suggestions (Optional):** If appropriate, suggest 1-2 potential next steps or follow-up questions the user might consider based on the findings.
5.  **Tone:** Be informative, objective, and helpful.
6.  **Output:** Respond with ONLY the summary text. Do not include greetings, introductions, or markdown formatting like headings or bullet points unless it naturally fits within the summary paragraph. Start directly with the summary.

Summary of Findings:
"""
        logger_api.debug(f"Gemini Summary Prompt:\n{summary_prompt[:600]}...") # Log beginning of prompt

        # --- Call Gemini API ---
        response = generate_with_key(
            2,  # use ENV GEMINI_API_KEY2 (second key)
            summary_prompt,
            GEMINI_REQUEST_TIMEOUT
        )

        # --- Process Response ---
        logger_api.debug(f"Gemini Raw Summary Response: {response}")
        generated_summary = response.strip()

        if not generated_summary:
             logger_api.warning("Gemini returned an empty summary.")
             return AISummaryResponse(error="AI failed to generate a summary.")

        logger_api.info(f"AI Summary generated successfully: {generated_summary[:100]}...")
        return AISummaryResponse(summary_text=generated_summary)

    # --- Error Handling ---
    except DeadlineExceeded:
        logger_api.error("Gemini API call for summary timed out.")
        raise HTTPException(status_code=504, detail="AI summary generation timed out.")
    except GoogleAPICallError as e:
        logger_api.error(f"Gemini API call error for summary: {e}", exc_info=True)
        raise HTTPException(status_code=502, detail=f"Error communicating with AI service for summary: {str(e)}")
    except Exception as e: # Generic catch for unexpected issues
        logger_api.error(f"Unexpected error during AI summary generation: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to generate AI summary: {str(e)}")
# +++ END NEW ENDPOINT +++


@bq_router.post("/suggest-visualization", response_model=SuggestVizResponse)
def suggest_visualization(req: SuggestVizRequest):
    """Suggests appropriate visualizations based on query results schema and context."""
    if not GEMINI_API_KEY:
        logger_api.warning("Visualization suggestion requested but Gemini API key not set.")
        return SuggestVizResponse(suggestions=[], error="AI suggestion service not configured.")

    if not req.schema_:
        raise HTTPException(status_code=400, detail="Schema information is required for suggestions.")

    logger_api.info(f"Received visualization suggestion request. Query: {req.query_sql[:50] if req.query_sql else 'N/A'}, Prompt: {req.prompt[:50] if req.prompt else 'N/A'}")

    try:
        # Prepare schema string
        schema_desc = "\n".join(
            [f"- Column '{s.get('name', 'Unknown')}' (Type: {s.get('type', '?')})" for s in req.schema_]
        )

        # Prepare sample string
        sample_str = ""
        if req.result_sample:
            try:
                sample_str = "\nResult Sample (first few rows):\n"
                if isinstance(req.result_sample, list) and req.result_sample and isinstance(req.result_sample[0], dict):
                    headers = list(req.result_sample[0].keys())
                    sample_str += "| " + " | ".join(headers) + " |\n"
                    sample_str += "|-" + "-|-".join(['-' * len(h) for h in headers]) + "-|\n"
                    for row in req.result_sample:
                        values = [str(row.get(h, '')) for h in headers]
                        sample_str += f"| {' | '.join(values)} |\n"
                else:
                    sample_str += "(Sample not available or in unexpected format)\n"
            except Exception as e_sample:
                logger_api.warning(f"Could not format result sample for prompt: {e_sample}")
                sample_str = "\n(Could not process sample data)\n"

        # Build prompt parts
        prompt_info = f"The query was generated from the natural language prompt: '{req.prompt}'" if req.prompt else ""
        sql_query_info = f"The SQL query executed was: ```sql\n{req.query_sql}\n```" if req.query_sql else ""

        prompt_context = f"""
A user ran a BigQuery query resulting in the following data schema:
{schema_desc}

{prompt_info}
{sql_query_info}
{sample_str}

Analyze the schema, query context (if provided), and sample data. Suggest suitable chart types for visualization.
For each suggestion, provide:
1.  `chart_type`: Choose ONE from "bar", "line", "pie", "scatter".
2.  `x_axis_column`: The EXACT column name from the schema to use for the X-axis (or categories for pie).
3.  `y_axis_columns`: A list containing ONE or MORE EXACT column names from the schema for the Y-axis (or values for pie/scatter). Use only numeric columns for Y-axis values.
4.  `rationale`: A SHORT explanation (max 1-2 sentences) why this chart is suitable.

Output ONLY a valid JSON object containing a single key "suggestions" which is a list of suggestion objects.
Example JSON Output:
{{
  "suggestions": [
    {{
      "chart_type": "bar",
      "x_axis_column": "product_category",
      "y_axis_columns": ["total_sales"],
      "rationale": "Compare sales across different product categories."
    }},
    {{
      "chart_type": "line",
      "x_axis_column": "order_date",
      "y_axis_columns": ["revenue", "profit"],
      "rationale": "Track revenue and profit trends over time."
    }}
  ]
}}
"""
        logger_api.debug(f"Gemini Viz Suggestion Prompt:\n{prompt_context[:500]}...")

        # Call Gemini with key index 2
        raw_json = generate_with_key(
            2,
            prompt_context,
            int(os.getenv("GEMINI_TIMEOUT_SECONDS", 120)) // 2
        )
        logger_api.debug(f"Raw Viz Suggestion JSON: {raw_json}")

        # Clean fences
        cleaned = raw_json.strip()
        if cleaned.startswith("```json"):
            cleaned = cleaned[len("```json"):].strip()
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3].strip()

        # Parse once
        try:
            data = json.loads(cleaned)
        except json.JSONDecodeError as e:
            logger_api.error(f"Failed to parse AI JSON: {e}\nContent: {cleaned}")
            return SuggestVizResponse(suggestions=[], error="AI response was not valid JSON.")

        # Validate structure
        if not isinstance(data, dict) or "suggestions" not in data or not isinstance(data["suggestions"], list):
            logger_api.error(f"Unexpected JSON structure: {data}")
            return SuggestVizResponse(suggestions=[], error="AI returned suggestions in an unexpected format.")

        # Build validated suggestions
        validated_suggestions = []
        schema_column_names = {s.get('name') for s in req.schema_ if s.get('name')}

        for sugg_raw in data["suggestions"]:
            if (
                isinstance(sugg_raw, dict)
                and all(k in sugg_raw for k in ["chart_type", "x_axis_column", "y_axis_columns", "rationale"])
                and isinstance(sugg_raw["y_axis_columns"], list)
                and isinstance(sugg_raw["x_axis_column"], str)
                and sugg_raw["x_axis_column"] in schema_column_names
                and all(isinstance(yc, str) and yc in schema_column_names for yc in sugg_raw["y_axis_columns"])
                and len(sugg_raw["y_axis_columns"]) > 0
            ):
                validated_suggestions.append(VizSuggestion(
                    chart_type=sugg_raw["chart_type"],
                    x_axis_column=sugg_raw["x_axis_column"],
                    y_axis_columns=sugg_raw["y_axis_columns"],
                    rationale=sugg_raw.get("rationale", "AI Suggestion.")
                ))
            else:
                logger_api.warning(f"Skipping invalid suggestion: {sugg_raw}")

        logger_api.info(f"Gemini generated {len(validated_suggestions)} valid visualization suggestions.")
        return SuggestVizResponse(suggestions=validated_suggestions)

    # Error handling
    except DeadlineExceeded:
        logger_api.error("Gemini API call for suggestions timed out.")
        raise HTTPException(status_code=504, detail="AI suggestion generation timed out.")
    except GoogleAPICallError as e:
        logger_api.error(f"Gemini API call error for suggestions: {e}", exc_info=True)
        raise HTTPException(status_code=502, detail=f"Error communicating with AI service for suggestions: {str(e)}")
    except Exception as e:
        logger_api.error(f"Unexpected error during visualization suggestion generation: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to generate suggestions: {str(e)}")

# --- Make sure you include the router in your main app ---
# Example: app.include_router(bq_router) should be present somewhere
# --- New Endpoint to List Datasets ---



@bq_router.get("/datasets", response_model=DatasetListResponse, tags=["BigQuery"])
def list_bigquery_datasets(
    user: dict = Depends(verify_token),
    bq_client: bigquery.Client = Depends(get_bigquery_client) # Inject client
    # filter_label: Optional[str] = Query(None, description="Filter datasets by label (e.g., 'env:prod')")
):
    """Retrieves a list of BigQuery datasets accessible by the service account."""
    user_uid = user.get("uid")
    if not user_uid:
        # This check is defensive, verify_token should handle missing uid
        logger_api.error("UID missing from verified token in /datasets endpoint.")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Authentication error: User ID not found.",
        )
    # if not api_bigquery_client:
    #     logger_api.error("Cannot list datasets: BigQuery client not available.")
    #     raise HTTPException(status_code=503, detail="BigQuery client not available.")

    # logger_api.info(f"Listing BigQuery datasets for project: {API_GCP_PROJECT}")
    # datasets_list: List[DatasetListItemModel] = []
    if not API_GCP_PROJECT: # Still need project ID from config
        logger_api.error("Cannot list datasets: GCP_PROJECT not configured.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Server configuration error (missing project ID).")

    logger_api.info(f"Listing BigQuery datasets for project: {API_GCP_PROJECT}")
    # datasets_list: List[DatasetListItemModel] = []
    try:
        user_role = get_user_role(user_uid)
        logger_api.info(f"User {user_uid} role determined as: {user_role}")
        # list_datasets returns an iterator of google.cloud.bigquery.DatasetListItem
        # datasets_list: List[DatasetListItemModel] = []
        all_datasets_from_bq: List[DatasetListItemModel] = []
        try:
            datasets_iterator = bq_client.list_datasets(project=API_GCP_PROJECT)
            for dataset_item in datasets_iterator:
                ds_ref = dataset_item.reference
                # Fetching full dataset info can be slow if there are many datasets.
                # Consider if just datasetId from list_datasets is enough,
                # but the model requires 'location'.
                try:
                    ds = bq_client.get_dataset(ds_ref)
                    all_datasets_from_bq.append(
                        DatasetListItemModel(
                            datasetId=ds.dataset_id,
                            location=ds.location
                        )
                    )
                except Exception as get_ds_error:
                     logger_api.warning(f"Could not get full info for dataset {ds_ref.dataset_id}, skipping: {get_ds_error}")

            logger_api.info(f"Fetched {len(all_datasets_from_bq)} datasets from BQ for project {API_GCP_PROJECT}")

        except GoogleAPICallError as e:
            logger_api.error(f"BQ API Error listing datasets from BQ: {e}")
            raise HTTPException(status_code=502, detail=f"Error communicating with BigQuery API: {str(e)}")
        except Exception as e:
             logger_api.error(f"Unexpected error listing datasets from BQ: {e}", exc_info=True)
             raise HTTPException(status_code=500, detail=f"Server error listing datasets: {str(e)}")


        # 3. Filter based on role/permissions
        final_datasets_list: List[DatasetListItemModel] = []

        if user_role == 'admin':
            logger_api.info(f"User {user_uid} is admin. Returning all {len(all_datasets_from_bq)} fetched datasets.")
            final_datasets_list = all_datasets_from_bq
# main.py - inside list_bigquery_datasets, before the filter
# ...
        else: # Not admin
            logger_api.info(f"User {user_uid} is not admin. Fetching accessible datasets from Firestore.")
            accessible_dataset_ids = get_user_accessible_datasets(user_uid)

            if accessible_dataset_ids is None:
                logger_api.warning(f"Could not determine accessible datasets for user {user_uid}. Returning empty list.")
                final_datasets_list = []
            elif not accessible_dataset_ids:
                logger_api.info(f"User {user_uid} has no datasets assigned in Firestore. Returning empty list.")
                final_datasets_list = []
            else:
                logger_api.info(f"User {user_uid} has access to Firestore dataset IDs: {accessible_dataset_ids}") # Log raw accessible IDs
                logger_api.info(f"BQ datasets fetched (short IDs): {[d.datasetId for d in all_datasets_from_bq]}") # Log BQ short IDs

                allowed_set = set(accessible_dataset_ids)
                final_datasets_list = [
                        ds for ds in all_datasets_from_bq if ds.datasetId in allowed_set
                ]
                if not final_datasets_list and all_datasets_from_bq: # If filtering resulted in empty but there were BQ datasets
                    logger_api.warning(f"FILTERING MISMATCH for user {user_uid}: BQ IDs: {[d.datasetId for d in all_datasets_from_bq]}, Firestore Allowed: {list(allowed_set)}")

                logger_api.info(f"Filtered list size for user {user_uid}: {len(final_datasets_list)}")
# ...

        # Sort alphabetically by datasetId before returning
        final_datasets_list.sort(key=lambda ds: ds.datasetId)

        return DatasetListResponse(datasets=final_datasets_list)

    except HTTPException as http_exc:
        # Re-raise HTTP exceptions from BQ calls or config errors
        raise http_exc
    except Exception as e:
        # Catch unexpected errors during role fetching or filtering logic
        logger_api.error(f"Unexpected error processing dataset list for user {user_uid}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected server error occurred while retrieving datasets.")
        # for dataset_item in api_bigquery_client.list_datasets(project=API_GCP_PROJECT):
        # for dataset_item in bq_client.list_datasets(project=API_GCP_PROJECT):
            # Extract the dataset ID
            # ds_ref: DatasetReference = dataset_item.reference
            # ds_ref: bigquery.DatasetReference = dataset_item.reference
            # Fetch full metadata (this is where location lives)
            # ds = api_bigquery_client.get_dataset(ds_ref)
            # ds = bq_client.get_dataset(ds_ref)
            # Optional: Add filtering logic here if needed based on labels, etc.
            # if filter_label and filter_label not in (dataset_item.labels or {}):
            #     continue
            # datasets_list.append(
            #     DatasetListItemModel(
            #         datasetId=ds.dataset_id,
            #         location=ds.location
            #     )
            # )
            # datasets_list.append(
            #     DatasetListItemModel(
            #         datasetId=ds.dataset_id,
            #         location=ds.location
            #     )
            # )

    #     logger_api.info(f"Found {len(datasets_list)} datasets.")
    #     return DatasetListResponse(datasets=datasets_list)
    # except GoogleAPICallError as e:
    #     logger_api.error(f"BQ API Error listing datasets: {e}")
    #     raise HTTPException(status_code=502, detail=f"Error communicating with BigQuery API: {str(e)}")
    # except Exception as e:
    #     logger_api.error(f"Unexpected error listing datasets: {e}", exc_info=True)
    #     raise HTTPException(status_code=500, detail=f"An unexpected error occurred while listing datasets: {str(e)}")
    # except GoogleAPICallError as e:
    #     logger_api.error(f"Google API Call Error listing datasets: {e}", exc_info=True)
    #     raise HTTPException(status_code=502, detail=f"Error communicating with BigQuery API: {str(e)}")
    # except Exception as e:
    #     logger_api.error(f"Unexpected error listing datasets: {e}", exc_info=True)
    #     raise HTTPException(status_code=500, detail=f"An unexpected error occurred while listing datasets: {str(e)}")
    





# --- BigQuery API Endpoints (using bq_router) ---

# ... (keep existing GET /datasets endpoint) ...
@bq_router.post(
    "/datasets",
    response_model=DatasetCreatedResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["BigQuery Workspaces"], # Renamed tag for clarity
    summary="Create a new BigQuery Workspace (Dataset)",
    description="Creates a new BigQuery workspace. Administrators can create multiple workspaces. Normal users are restricted to creating a single workspace to which they get automatic access.",
    # Removed: dependencies=[Depends(require_admin)] # Role logic is now handled inside
)

@bq_router.post(
    "/datasets",
    response_model=DatasetCreatedResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["BigQuery Workspaces"],
    summary="Create a new BigQuery Workspace (Dataset)",
    description="Creates a new BigQuery workspace...",
)
def create_bigquery_workspace(
    req: CreateDatasetRequest,
    bq_client: bigquery.Client = Depends(get_bigquery_client),
    user: dict = Depends(verify_token), # user is the decoded token from verify_token
):
    user_uid = user.get("uid")
    user_email = user.get("email") # Assuming your token contains email
    # display_name might also be in the token, or you might not store it.
    # user_display_name = user.get("name") 

    if not user_uid:
        logger_api.error("User UID not found in token during workspace creation attempt.")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication error: User ID not found.")

    # +++ ENSURE USER DOCUMENT EXISTS IN FIRESTORE +++
    # Pass email if available from the token, it's good to store it.
    doc_ensured =  ensure_user_document_exists(user_uid, email=user_email)
    if not doc_ensured:
        logger_api.error(f"Failed to ensure Firestore document exists for user '{user_uid}'. Cannot proceed with workspace creation.")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="User profile could not be initialized. Please try again or contact support."
        )
    # ++++++++++++++++++++++++++++++++++++++++++++++++

    user_role =  get_user_role(user_uid)
    # If user_role is still None here, it means ensure_user_document_exists might have created it,
    # but get_user_role fetched before a potential update, OR ensure_user_document_exists failed to add 'role'
    # The updated ensure_user_document_exists tries to mitigate this.
    # A fresh call to get_user_role *after* ensure_user_document_exists should be reliable.
    if user_role is None: # Should ideally not happen if doc_ensured is True
        logger_api.error(f"User role is None for {user_uid} even after ensuring document. This is unexpected.")
        # Re-fetch role just in case there was a slight delay or if ensure_user_document_exists just created it
        user_role =  get_user_role(user_uid)
        if user_role is None:
             raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Could not determine user role.")


    logger_api.info(f"Workspace creation attempt by user '{user_uid}' (Role: '{user_role}') for workspace ID '{req.dataset_id}'.")

    if user_role != 'admin':
        accessible_datasets =  get_user_accessible_datasets(user_uid)
        if accessible_datasets is None:
            logger_api.error(f"Could not verify workspace access for user '{user_uid}' due to Firestore error (accessible_datasets is None).")
            # This path should be less likely now if ensure_user_document_exists ran successfully
            # because get_user_accessible_datasets returns [] if doc exists but field is missing.
            # So, `None` here would imply a more fundamental issue during the fetch itself.
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Could not verify existing workspace access. Please try again.")
        
        if accessible_datasets: # If the list is not empty
            logger_api.warning(
                f"User '{user_uid}' (Role: '{user_role}') attempted to create workspace '{req.dataset_id}' "
                f"but already has access to {len(accessible_datasets)} workspace(s). Creation denied."
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You are limited to creating/accessing one workspace initially. Please contact an administrator if you need access to more."
            )
        logger_api.info(f"User '{user_uid}' (Role: '{user_role}') is a normal user and currently has access to 0 workspaces. Creation allowed.")

    # ... (rest of your create_bigquery_workspace function remains the same) ...
    if not API_GCP_PROJECT:
        logger_api.error("Cannot create workspace: API_GCP_PROJECT not configured.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Server configuration error: Missing project ID.")

    dataset_ref = bigquery.DatasetReference(API_GCP_PROJECT, req.dataset_id)
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = req.location or DEFAULT_BQ_LOCATION
    if req.description: dataset.description = req.description
    if req.labels: dataset.labels = req.labels

    created_bq_dataset_obj = None
    try:
        created_bq_dataset_obj = bq_client.create_dataset(dataset, timeout=30, exists_ok=False)
        logger_api.info(f"User '{user_uid}' successfully created BigQuery dataset: {created_bq_dataset_obj.full_dataset_id}")

        # Register in Firestore and grant access to creator
        firestore_success =  register_workspace_and_grant_access(
            dataset_id=created_bq_dataset_obj.dataset_id, # Pass short ID
            owner_uid=user_uid,
            location=created_bq_dataset_obj.location,
            description=created_bq_dataset_obj.description,
            labels=created_bq_dataset_obj.labels
        )

        if not firestore_success:
            logger_api.error(
                f"CRITICAL: BigQuery dataset '{created_bq_dataset_obj.dataset_id}' created by user '{user_uid}', "
                f"but FAILED to register in Firestore or grant access. Attempting BigQuery dataset cleanup."
            )
            try:
                bq_client.delete_dataset(dataset_ref, delete_contents=True, not_found_ok=True)
                logger_api.info(f"Successfully cleaned up BigQuery dataset '{created_bq_dataset_obj.dataset_id}' after Firestore registration failure.")
            except Exception as cleanup_error:
                logger_api.error(
                    f"CRITICAL ROLLBACK FAILURE: FAILED to clean up BigQuery dataset '{created_bq_dataset_obj.dataset_id}': {cleanup_error}."
                )
            # This situation is tricky. The BQ dataset exists but Firestore is inconsistent.
            # Forcing a 500 error is one way to signal a problem that needs attention.
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Workspace creation partially failed. Please contact support.")

        return DatasetCreatedResponse(
            project_id=created_bq_dataset_obj.project,
            dataset_id=created_bq_dataset_obj.dataset_id,
            location=created_bq_dataset_obj.location,
            description=created_bq_dataset_obj.description,
            labels=created_bq_dataset_obj.labels,
        )
    except BadRequest as e: # e.g. invalid dataset_id format
        logger_api.warning(f"Bad request error creating workspace '{req.dataset_id}' for user '{user_uid}': {e}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid request for workspace creation: {str(e)}")
    except Conflict: # Dataset already exists in BQ
        logger_api.warning(f"Conflict: Workspace ID '{req.dataset_id}' already exists in BigQuery. Attempt by user '{user_uid}'.")
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"The workspace ID '{req.dataset_id}' already exists.")
    except GoogleAPICallError as e:
        logger_api.error(f"Google API Error creating BigQuery dataset '{req.dataset_id}' by '{user_uid}': {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="Error with Google Cloud services during workspace creation.")
    except Exception as e: # Catch-all for other unexpected errors
        logger_api.error(f"Unexpected error creating workspace '{req.dataset_id}' by '{user_uid}': {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Unexpected server error during workspace creation.")



# main.py
# ... other imports ...

@bq_router.delete(
    "/datasets/{dataset_id_only}/tables/{table_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    tags=["BigQuery Tables"], # Changed tag for clarity
    summary="Delete a specific BigQuery Table",
    description="Permanently deletes a specific table within a workspace. Admins can delete any table. Normal users can only delete tables within workspaces they have access to.",
    # Removed: dependencies=[Depends(require_admin)], # RBAC is handled inside
    # ... (responses remain the same) ...
)
def delete_bigquery_table(
    dataset_id_only: str = Path(..., description="The ID of the workspace (dataset name only)."),
    table_id: str = Path(..., description="The ID of the table to delete."),
    user: dict = Depends(verify_token), # Get current user
    bq_client: bigquery.Client = Depends(get_bigquery_client),
):
    user_uid = user.get("uid")
    if not user_uid: # Should be caught by verify_token
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication error.")

    if not API_GCP_PROJECT:
        logger_api.error(f"User {user_uid}: Cannot delete table '{table_id}' in dataset '{dataset_id_only}': API_GCP_PROJECT not configured.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Server configuration error.")

    # Check user's role and access
    user_role =  get_user_role(user_uid)
    can_delete = False
    if user_role == 'admin':
        can_delete = True
        logger_api.info(f"Admin '{user_uid}' attempting to delete table '{table_id}' from workspace '{dataset_id_only}'.")
    else: # Normal user
        accessible_datasets =  get_user_accessible_datasets(user_uid)
        if accessible_datasets is None: # Firestore error
             logger_api.error(f"User '{user_uid}': Could not verify access to workspace '{dataset_id_only}' for table deletion due to Firestore error.")
             raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Could not verify workspace access.")
        if dataset_id_only in accessible_datasets:
            can_delete = True
            logger_api.info(f"User '{user_uid}' has access to workspace '{dataset_id_only}', attempting to delete table '{table_id}'.")
        else:
            logger_api.warning(f"User '{user_uid}' does not have access to workspace '{dataset_id_only}'. Deletion of table '{table_id}' denied.")
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f"You do not have permission to delete tables in workspace '{dataset_id_only}'.")
    
    if not can_delete: # Should not be reached if logic above is correct, but as a safeguard
         raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Permission denied to delete this table.")

    full_table_id = f"{API_GCP_PROJECT}.{dataset_id_only}.{table_id}"
    logger_api.warning(f"User '{user_uid}' (Role: {user_role}) is proceeding to DELETE table: {full_table_id}")

    try:
        bq_client.delete_table(full_table_id, not_found_ok=False)
        logger_api.info(f"User '{user_uid}': Successfully deleted table: {full_table_id}")
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except NotFound:
        logger_api.warning(f"User '{user_uid}': Table or dataset not found during delete attempt: {full_table_id}")
        # ... (existing NotFound logic to check if dataset or table is missing) ...
        try:
            bq_client.get_dataset(f"{API_GCP_PROJECT}.{dataset_id_only}")
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Table '{table_id}' not found in workspace '{dataset_id_only}'.")
        except NotFound:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Workspace '{dataset_id_only}' not found.")
        except Exception as ds_check_err:
            logger_api.error(f"Error checking dataset existence for {full_table_id}: {ds_check_err}")
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table or Workspace not found (verification error).")
    except GoogleAPICallError as e:
        logger_api.error(f"User '{user_uid}': Google API Error deleting table '{full_table_id}': {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=f"Error communicating with Google Cloud during table deletion.")
    except Exception as e:
        logger_api.error(f"User '{user_uid}': Unexpected error deleting table '{full_table_id}': {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Unexpected server error during table deletion.")
# +++ END NEW ENDPOINT: Delete Dataset +++




@bq_router.post("/suggest-prompt", response_model=PromptSuggestionResponse)
def suggest_prompt_completion(req: PromptSuggestionRequest):
    """
    Provides AI-generated suggestions to complete or improve a natural language prompt
    for data analysis.
    """
    if not GEMINI_API_KEY:
        logger_api.warning("Prompt suggestion requested but Gemini API key not set.")
        # Return empty list gracefully, frontend can handle lack of suggestions
        return PromptSuggestionResponse(suggestions=[], error="AI suggestion service not configured.")

    if not req.current_prompt or len(req.current_prompt.strip()) < 3:
         # Don't bother AI with very short/empty prompts
         return PromptSuggestionResponse(suggestions=[])

    logger_api.info(f"Received prompt suggestion request for: '{req.current_prompt}'")

    # --- Construct the AI Prompt ---
    # This prompt asks the AI to act as a prompt helper
    suggestion_ai_prompt = f"""You are an assistant helping a user write clear natural language prompts for data analysis (e.g., to query BigQuery). The user is currently typing the following:

"{req.current_prompt}"

Suggest 2-4 concise ways to complete or improve this prompt to make it more specific and effective for data analysis. Focus on clarity, mentioning potential metrics (like 'total', 'average'), dimensions (like 'per category', 'over time'), or timeframes ('last month', 'yesterday'). Do NOT generate SQL code.

Return ONLY a valid JSON array of strings, where each string is a suggested prompt. Example format:
["show total sales per product category", "show average order value by month"]

JSON Array of Suggestions:
"""

    try:
        # --- Call Gemini API ---
        # Using a model optimized for fast responses is good here
        response = generate_with_key(
            2,  # use ENV GEMINI_API_KEY (first key)
            suggestion_ai_prompt,
            GEMINI_REQUEST_TIMEOUT // 2
        )

        # --- Process Response ---
        logger_api.debug(f"Gemini Raw Suggestion Response: {response}")
        suggestions = []
        error_msg = None
        try:
            # Clean potential markdown ```json ... ``` artifacts
            cleaned_response = response.strip()
            if cleaned_response.startswith("```json"):
                cleaned_response = cleaned_response[len("```json"):].strip()
            if cleaned_response.endswith("```"):
                cleaned_response = cleaned_response[:-len("```")].strip()

            if not cleaned_response:
                 logger_api.warning("Gemini returned empty string for prompt suggestions.")
                 error_msg = "AI returned no suggestions."
            else:
                # Parse the JSON array
                parsed_suggestions = json.loads(cleaned_response)
                if isinstance(parsed_suggestions, list) and all(isinstance(s, str) for s in parsed_suggestions):
                    suggestions = parsed_suggestions
                    logger_api.info(f"Generated {len(suggestions)} prompt suggestions.")
                else:
                    logger_api.warning(f"Gemini response was not a valid JSON array of strings: {parsed_suggestions}")
                    error_msg = "AI response format was unexpected."

        except json.JSONDecodeError as json_err:
             logger_api.error(f"Failed to parse JSON response from Gemini for suggestions: {json_err}\nResponse Text: {response}")
             error_msg = "AI response was not valid JSON."
        except Exception as e_parse:
             logger_api.error(f"Error processing Gemini suggestions response: {e_parse}", exc_info=True)
             error_msg = "Error processing AI suggestions."

        return PromptSuggestionResponse(suggestions=suggestions, error=error_msg)

    # --- Error Handling for API Call ---
    except DeadlineExceeded:
        logger_api.error("Gemini API call for prompt suggestions timed out.")
        # Don't raise HTTPException, return error in response body
        return PromptSuggestionResponse(suggestions=[], error="AI suggestion request timed out.")
    except GoogleAPICallError as e:
        logger_api.error(f"Gemini API call error for prompt suggestions: {e}", exc_info=True)
        return PromptSuggestionResponse(suggestions=[], error="Error communicating with AI service.")
    except Exception as e:
        logger_api.error(f"Unexpected error during prompt suggestion generation: {e}", exc_info=True)
        return PromptSuggestionResponse(suggestions=[], error="Failed to generate suggestions.")
# +++ END NEW ENDPOINT: Create Dataset +++

@bq_router.delete(
    "/datasets/{dataset_id_only}/tables/{table_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    tags=["BigQuery", "Admin"], # Add Admin tag maybe
    summary="Delete a specific BigQuery Table (Admin Only)",
    description="Permanently deletes a specific table within a dataset. Requires administrator privileges.",
    dependencies=[Depends(require_admin)], # Apply RBAC
    responses={
        204: {"description": "Table deleted successfully"},
        403: {"description": "Permission denied: Admin role required"},
        404: {"description": "Dataset or Table not found"},
        500: {"description": "Internal server error"},
        502: {"description": "Error communicating with BigQuery API"},
        503: {"description": "BigQuery service unavailable"}
    }
)
def delete_bigquery_table(
    dataset_id_only: str = Path(..., description="The ID of the dataset (workspace name only, e.g., 'MVP').", example="MVP"),
    table_id: str = Path(..., description="The ID of the table to delete.", example="Master_Data_Employee_Records_backup"),
    admin_user: dict = Depends(require_admin), # Get admin user data for logging
    bq_client: bigquery.Client = Depends(get_bigquery_client) # Inject client
):
    """
    Deletes an existing BigQuery table within a specified dataset. (Admin Only)
    Requires the requesting user to have the 'admin' role stored in Firestore.
    """
    admin_uid = admin_user.get("uid", "unknown_admin")
    if not API_GCP_PROJECT:
        logger_api.error(f"Admin {admin_uid}: Cannot delete table '{table_id}' in dataset '{dataset_id_only}': GCP_PROJECT not configured.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Server configuration error (missing project ID).")

    # Construct the full table ID: project.dataset.table
    full_table_id = f"{API_GCP_PROJECT}.{dataset_id_only}.{table_id}"
    logger_api.warning(f"ADMIN ACTION by UID {admin_uid}: Attempting to DELETE table: {full_table_id}")

    try:
        # Delete the table.
        # not_found_ok=False: Raises NotFound exception if the table doesn't exist.
        bq_client.delete_table(full_table_id, not_found_ok=False)

        logger_api.info(f"Admin {admin_uid}: Successfully deleted table: {full_table_id}")
        # Return HTTP 204 No Content on success
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    except NotFound:
        logger_api.warning(f"Admin {admin_uid}: Table or dataset not found during delete attempt: {full_table_id}")
        # Check if dataset exists to give slightly better error
        try:
            bq_client.get_dataset(f"{API_GCP_PROJECT}.{dataset_id_only}")
            # If dataset exists, the table must be missing
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Table '{table_id}' not found in dataset '{dataset_id_only}'.")
        except NotFound:
             # Dataset itself is missing
             raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Dataset '{dataset_id_only}' not found.")
        except Exception as ds_check_err: # Catch errors during dataset check
             logger_api.error(f"Error checking dataset existence during table delete for {full_table_id}: {ds_check_err}")
             raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Table '{table_id}' or Dataset '{dataset_id_only}' not found (verification error).")

    except GoogleAPICallError as e:
        # Catch specific BQ API errors (e.g., permission issues on the *service account*)
        logger_api.error(f"Admin {admin_uid}: Google API Call Error deleting table '{full_table_id}': {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=f"Error communicating with BigQuery API during table deletion: {str(e)}")
    except Exception as e:
        # Catch any other unexpected errors
        logger_api.error(f"Admin {admin_uid}: Unexpected error deleting table '{full_table_id}': {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected server error occurred during table deletion: {str(e)}")
# ... (rest of the existing endpoints like /schema, /nl2sql, /jobs, etc.) ...


@bq_router.delete(
    "/datasets/{dataset_id}", # Path parameter is just {dataset_id}
    status_code=status.HTTP_204_NO_CONTENT,
    tags=["BigQuery Workspaces"],
    summary="Delete a BigQuery Workspace (Dataset) (Admin Only)",
    description="Permanently deletes a BigQuery workspace and all its contents. Requires administrator privileges.",
    dependencies=[Depends(require_admin)], 
    responses={
        204: {"description": "Workspace deleted successfully"},
        403: {"description": "Permission denied: Admin role required"},
        404: {"description": "Workspace not found"},
        # ... other responses
    }
)
def delete_bigquery_workspace( 
    dataset_id: str = Path(..., description="The ID of the workspace (dataset name only) to delete.", example="my_team_workspace_to_delete"),
    admin_user: dict = Depends(require_admin), 
    bq_client: bigquery.Client = Depends(get_bigquery_client),
):
    admin_uid = admin_user.get("uid", "unknown_admin")
    if not API_GCP_PROJECT:
        logger_api.error(f"Admin {admin_uid}: Cannot delete workspace '{dataset_id}': API_GCP_PROJECT not configured.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Server configuration error.")

    full_dataset_id_for_bq = f"{API_GCP_PROJECT}.{dataset_id}" 

    logger_api.warning(f"ADMIN ACTION by UID {admin_uid}: Attempting to DELETE workspace: {full_dataset_id_for_bq} (path param was: {dataset_id})")

    try:
        dataset_ref = bigquery.DatasetReference(API_GCP_PROJECT, dataset_id) 

        # 1. Delete from BigQuery first
        bq_client.delete_dataset(
            dataset_ref, delete_contents=True, not_found_ok=False
        )
        logger_api.info(f"Admin {admin_uid}: Successfully deleted workspace from BigQuery: {full_dataset_id_for_bq}")
        
        # 2. --- CALL THE NEW FIRESTORE CLEANUP FUNCTION ---
        # This should happen *after* successful BQ deletion.
        # Pass the short dataset_id.
        try:
            firestore_cleanup_success =  remove_workspace_from_firestore(dataset_id)
            if firestore_cleanup_success:
                logger_api.info(f"Admin {admin_uid}: Successfully removed workspace '{dataset_id}' records from Firestore.")
            else:
                # Log an error but don't necessarily fail the whole operation if BQ delete succeeded.
                # This indicates a partial success and might require manual Firestore cleanup.
                logger_api.error(f"Admin {admin_uid}: Workspace '{dataset_id}' deleted from BigQuery, "
                                 f"but FAILED to remove all records from Firestore. Manual cleanup may be needed.")
        except Exception as fs_delete_error:
            logger_api.error(f"Admin {admin_uid}: Exception during Firestore cleanup for workspace '{dataset_id}' "
                             f"after BQ deletion: {fs_delete_error}", exc_info=True)
            # Still, BQ deletion was successful, so we proceed.

        return Response(status_code=status.HTTP_204_NO_CONTENT)

    except NotFound: # This is for BigQuery NotFound
        logger_api.warning(f"Admin {admin_uid}: Workspace not found in BigQuery during delete attempt: {full_dataset_id_for_bq}")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Workspace '{dataset_id}' not found in project '{API_GCP_PROJECT}'.")
    except GoogleAPICallError as e:
        logger_api.error(f"Admin {admin_uid}: Google API Error deleting workspace '{full_dataset_id_for_bq}': {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=f"Error communicating with Google Cloud during workspace deletion: {str(e)}")
    except Exception as e:
        logger_api.error(f"Admin {admin_uid}: Unexpected error deleting workspace '{full_dataset_id_for_bq}': {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected server error occurred during workspace deletion: {str(e)}")
    


# --- NEW /api/internal/etl-file-completed (called by worker) ---
@app.post("/api/internal/etl-file-completed", tags=["ETL Internal"], include_in_schema=False)
def worker_reports_file_completion(payload: WorkerFileCompletionPayload, background_tasks: BackgroundTasks):
    # Add simple API Key auth for this internal endpoint
    # api_key = request.headers.get("X-Worker-API-Key")
    # if api_key != YOUR_WORKER_SECRET_KEY:
    #     raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid worker API key")

    logger_api.info(f"Worker reporting completion: Batch {payload.batch_id}, File {payload.file_id}, Success: {payload.success}")
    # Use background task for Firestore update to respond to worker quickly
    background_tasks.add_task(update_file_status_in_firestore, payload)
    return {"message": "Status received"}


# --- Modified /api/etl-batch-status/{batch_id} ---
@app.get("/api/etl-batch-status/{batch_id}", response_model=Dict[str, Any], tags=["ETL"], dependencies=[Depends(verify_token)])
def get_etl_batch_status(batch_id: str, current_user: dict = Depends(verify_token)):
    user_uid = current_user.get("uid")
    logger_api.debug(f"User {user_uid} polling status for batch_id: {batch_id}")
    client = get_firestore_client()
    batch_doc_ref = client.collection(ETL_BATCHES_COLLECTION).document(batch_id)
    batch_snapshot = batch_doc_ref.get()

    if not batch_snapshot.exists:
        raise HTTPException(status_code=404, detail="Batch ID not found.")

    batch_data = batch_snapshot.to_dict()
    # Security check: Ensure the current user owns this batch
    if batch_data.get("userId") != user_uid:
        logger_api.warning(f"User {user_uid} attempted to access batch {batch_id} owned by {batch_data.get('userId')}")
        raise HTTPException(status_code=403, detail="Forbidden: You do not have access to this batch status.")

    return batch_data

# --- NEW /api/user-active-etl-batches ---
@app.get("/api/user-active-etl-batches", response_model=List[Dict[str, Any]], tags=["ETL"], dependencies=[Depends(verify_token)])
def get_user_active_etl_batches(current_user: dict = Depends(verify_token)):
    user_uid = current_user.get("uid")
    active_batches_data = []
    try:
        client = get_firestore_client()
        query = client.collection(ETL_BATCHES_COLLECTION) \
            .where(filter=FieldFilter("userId", "==", user_uid)) \
            .where(filter=FieldFilter("overallBatchStatus", "in", ["processing", "queued_for_trigger", "triggered_to_worker"])) \
            .order_by("creationTime", direction='DESCENDING') \
            .limit(10) # Limit to avoid fetching too many old "stuck" batches

        for doc_snapshot in query.stream(): # Use async for loop
            batch_data = doc_snapshot.to_dict()
            batch_data["batch_id"] = doc_snapshot.id # Add batch_id to the returned data
            active_batches_data.append(batch_data)

        logger_api.info(f"Found {len(active_batches_data)} active/pending batches for user {user_uid}")
        return active_batches_data
    except Exception as e:
        logger_api.error(f"Error fetching active ETL batches for user {user_uid}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve active batch statuses.")





# If you want to keep feedback related things together, add to feedback_router
# Otherwise, it can be directly under app or its own router.
# For this example, let's add it under a general app path or a new utility router.




# Inside main.py

@app.get( 
    "/api/feedback-image-upload-url",
    response_model=FeedbackImageUploadUrlResponse,
    tags=["Feedback Utilities"], 
    dependencies=[Depends(verify_token)] 
)
def get_feedback_image_upload_url(
    filename: str = Query(..., description="The original name of the image file."), # Get as query param
    content_type: str = Query(..., description="The MIME type of the image (e.g., image/jpeg)."), # Get as query param
    current_user: dict = Depends(verify_token),
    storage_client: storage.Client = Depends(get_storage_client) 
):
    user_uid = current_user.get("uid")
    if not user_uid: # Should be caught by verify_token
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not authenticated.")

    if not FEEDBACK_GCS_BUCKET: 
        logger_api.error("Cannot generate feedback image upload URL: API_GCS_BUCKET not configured.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Server storage configuration error.")

    # Sanitize filename received from query parameter
    clean_filename = re.sub(r"[^a-zA-Z0-9_.-]", "_", os.path.basename(filename))
    
    gcs_object_name = f"feedback_attachments/{user_uid}/{uuid.uuid4()}_{clean_filename}"

    try:
        bucket = storage_client.bucket(FEEDBACK_GCS_BUCKET) 
        blob = bucket.blob(gcs_object_name)

        upload_url = blob.generate_signed_url(
            version="v4",
            expiration=timedelta(minutes=15), 
            method="PUT",
            content_type=content_type, # Use content_type from query parameter
        )
        
        logger_api.info(f"Generated signed URL for feedback image: {gcs_object_name} for user {user_uid}")
        return FeedbackImageUploadUrlResponse(
            upload_url=upload_url,
            gcs_object_name=gcs_object_name
        )
    except Exception as e:
        logger_api.error(f"Error generating signed URL for feedback image {filename} for user {user_uid}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Could not generate image upload URL.")


# main.py
# ...
# feedback_router = APIRouter(...) # Assuming you have this defined

@feedback_router.post("/", response_model=FeedbackResponse, status_code=status.HTTP_201_CREATED)
def submit_user_feedback(
    feedback_submission: FeedbackSubmission,
    current_user: Dict[str, Any] = Depends(verify_token) # verify_token provides user_uid
):
    user_uid = current_user.get("uid")
    if not user_uid:
        # This should ideally be caught by verify_token, but as a safeguard
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not authenticated.")

    logger_api.info(f"Received feedback submission from user: {user_uid}, type: {feedback_submission.feedback_type}")
    
    # feedback_submission.model_dump(exclude_none=True) will include 'image_urls' if present in the request.
    # Your store_feedback service needs to be able to handle this list of URLs.
    feedback_data_to_store = feedback_submission.model_dump(exclude_none=True)
    
    logger_api.debug(f"Data being passed to store_feedback: {feedback_data_to_store}")

    # Ensure your store_feedback service can handle the 'image_urls' field
    # and save it appropriately (e.g., as an array in Firestore).
    feedback_id = store_feedback(user_uid, feedback_data_to_store)

    if feedback_id:
        return FeedbackResponse(message="Feedback submitted successfully.", feedback_id=feedback_id)
    else:
        # This could happen if store_feedback returns None or raises an exception
        logger_api.error(f"Failed to store feedback for user {user_uid}. store_feedback returned no ID.")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Could not store feedback at this time. Please try again later."
        )

# ...



# app.include_router(feedback_router) # Make sure router is included






# --- Include Routers ---
app.include_router(bq_router)
app.include_router(chat_router)
app.include_router(export_router) 
app.include_router(user_profile_router) # +++ Include the new user profile router +++
app.include_router(feedback_router)
# --- Uvicorn Runner ---
if __name__ == "__main__":
    import uvicorn
    api_host = os.getenv("API_HOST", "127.0.0.1")
    api_port = int(os.getenv("API_PORT", 8000))
    logger_api.info(f"Starting Uvicorn server on http://{api_host}:{api_port}")
    uvicorn.run("main:app", host=api_host, port=api_port, reload=False)
