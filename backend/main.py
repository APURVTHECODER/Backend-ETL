# ==============================================================================
# SECTION 1: FastAPI BigQuery Job Runner + Upload/ETL Trigger + AI Features
# ==============================================================================
import os
import logging
import tempfile
import atexit # To help clean up the temp file
import re
from datetime import timedelta, timezone, datetime, date, time # Added date, time
from typing import List, Dict, Any, Optional, Union
import traceback
import uuid
from routers.chatbot import chat_router
from auth import get_current_user, verify_token
# FastAPI and Pydantic
import base64
from clients import initialize_google_clients, initialize_gemini, _cleanup_temp_cred_file_clients # +++ MODIFIED +++
from fastapi import FastAPI, HTTPException, Request, APIRouter, Depends, Query, Path , status,Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse # Keep if used later
from pydantic import BaseModel, Field
import json
# Google Cloud Libraries
from google.cloud import bigquery, storage, pubsub_v1
from google.cloud.exceptions import NotFound, BadRequest , Conflict
from google.api_core.exceptions import GoogleAPICallError, DeadlineExceeded
from google.oauth2 import service_account
import google.generativeai as genai
from routers.export import export_router
from services.firestore_service import initialize_firestore,get_user_accessible_datasets,get_user_role # Import initializer
from dependencies.rbac import require_admin # Import RBAC dependency
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
# Utilities
from dotenv import load_dotenv
import pandas as pd

# --- Load Environment Variables ---
load_dotenv()

# --- FastAPI App Initialization ---
app = FastAPI(title="Intelligent BigQuery & ETL API")
bq_router = APIRouter(
    prefix="/api/bigquery",
    tags=["BigQuery"],
    dependencies=[Depends(verify_token)] # Protect all BQ routes
)

# --- Logging Setup ---
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=log_level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger_api = logging.getLogger("uvicorn.error" if "uvicorn" in os.getenv("SERVER_SOFTWARE", "") else __name__ + "_api")
logger_api.setLevel(log_level)
logger_api.info("FastAPI application starting...")


@app.on_event("startup")
async def startup_event():
    logger_api.info("Running startup event: Initializing Gemini and ensuring credentials setup...")
    # Initialize Gemini directly if needed globally
    initialize_gemini()
    from services.firestore_service import initialize_firestore
    initialize_firestore()
    # We still call initialize_google_clients to ensure the temp file
    # and environment variable are set up correctly before the first request.
    # The actual client object creation will be triggered by the first dependency call.
    initialize_google_clients()
    atexit.register(_cleanup_temp_cred_file_clients)
    logger_api.info("Startup pre-initialization complete.")



allowed_origins = os.getenv("ALLOWED_ORIGINS", "*").split(",")
app.add_middleware(CORSMiddleware, allow_origins=allowed_origins, allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
logger_api.info(f"CORS enabled for origins: {allowed_origins}")

# --- Pydantic Models ---
# --- Pydantic Models for Prompt Suggestion ---
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



async def get_dataset_schema(dataset_id: str, bq_client: bigquery.Client) -> List[TableSchema]:
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
async def get_bigquery_dataset_schema_endpoint( # Renamed function
    dataset_id: str = Query(..., description="Full dataset ID (e.g., project.dataset)"),
    bq_client: bigquery.Client = Depends(get_bigquery_client) # Inject client
):
    """Retrieves the schema (table names, column names, types) for all tables in a dataset."""
    # Consider adding caching here later for performance (e.g., using Redis or in-memory cache)
    try:
        # table_schemas = get_dataset_schema(dataset_id)
        table_schemas = await get_dataset_schema(dataset_id, bq_client) # Pass client
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
async def natural_language_to_sql(
    req: NLQueryRequest,
    bq_client: bigquery.Client = Depends(get_bigquery_client) # Inject client
):
    """Converts a natural language prompt into a BigQuery SQL query using Gemini, potentially filtered by table prefix."""
    # if not api_bigquery_client: raise HTTPException(503, "BigQuery client not available.")
    if not GEMINI_API_KEY: raise HTTPException(503, "NL-to-SQL service not configured.")
    if not req.prompt or req.prompt.isspace(): raise HTTPException(400, "Prompt required.")
    logger_api.info(f"NL-to-SQL request for dataset '{req.dataset_id}'.")
    # if not req.prompt or req.prompt.isspace(): raise HTTPException(400, "Prompt cannot be empty.")

    # logger_api.info(f"NL-to-SQL request for dataset '{req.dataset_id}'. Prefix: '{req.table_prefix or 'None'}'. Prompt: {req.prompt[:100]}...")

# Inside natural_language_to_sql function

    logger_api.info(f"NL-to-SQL request for dataset '{req.dataset_id}'. Mode: {req.ai_mode}.")
    if req.ai_mode == "SEMI_AUTO":
        logger_api.info(f"  Selected Tables: {req.selected_tables}")
        logger_api.info(f"  Selected Columns: {req.selected_columns}")

    try:
        # 1. Fetch the *full* schema for the dataset first
        all_table_schemas_full = await get_dataset_schema(req.dataset_id, bq_client)
        if not all_table_schemas_full:
            raise HTTPException(404, f"No schema found or dataset empty: {req.dataset_id}.")

        import copy
        all_table_schemas = copy.deepcopy(all_table_schemas_full)

        # 2. Determine the schema to send to the AI based on mode
        schema_to_use: List[TableSchema] = []
        schema_source_description = "all tables" # For logging/prompt

# --- START: Corrected SEMI_AUTO Logic Block ---
        if req.ai_mode == "SEMI_AUTO":
            if not req.selected_tables:
                logger_api.warning("SEMI_AUTO mode selected but no tables provided.")
                raise HTTPException(400, "SEMI_AUTO mode requires at least one table to be selected.")

            # 1. --- Filter by selected tables FIRST ---
            selected_tables_set = set(req.selected_tables) # Use set for efficiency
            schema_to_use = [ts for ts in all_table_schemas if ts.table_id in selected_tables_set]
            schema_source_description = f"selected tables ({len(schema_to_use)}): {', '.join(req.selected_tables)}"
            logger_api.info(f"SEMI_AUTO: Filtered to {len(schema_to_use)} tables based on selection.")

            # Check if any tables were actually found before proceeding
            if not schema_to_use:
                 logger_api.warning(f"SEMI_AUTO: No tables found matching the selection: {req.selected_tables}")
                 raise HTTPException(400, "None of the selected tables were found in the dataset schema.")

            # 2. --- THEN, filter columns within the selected tables (if columns are provided) ---
            if req.selected_columns:
                filtered_schema_with_cols: List[TableSchema] = []
                selected_columns_set = set(req.selected_columns)
                logger_api.debug(f"SEMI_AUTO: Filtering columns within selected tables against: {selected_columns_set}")

                # Now iterate through the *already table-filtered* schema_to_use list
                for ts in schema_to_use:
                    original_column_count = len(ts.columns)
                    original_column_names = {c.name for c in ts.columns}

                    # Filter columns for this specific table
                    ts.columns = [c for c in ts.columns if c.name in selected_columns_set]
                    kept_column_names = {c.name for c in ts.columns}

                    logger_api.info(f"  Table '{ts.table_id}': Kept columns after filtering: {list(kept_column_names)}")
                    # Optional: Log dropped columns
                    # dropped_column_names = original_column_names - kept_column_names
                    # if dropped_column_names:
                    #     logger_api.debug(f"    (Dropped columns for this table: {list(dropped_column_names)})")


                    # Keep the table in the final list ONLY if it still has columns after filtering
                    if ts.columns:
                        filtered_schema_with_cols.append(ts)
                    else:
                        logger_api.info(f"  Table '{ts.table_id}': Removed as no selected columns remained after filtering.")

                # Update schema_to_use with the column-filtered list
                schema_to_use = filtered_schema_with_cols
                schema_source_description += f", focusing on columns: {', '.join(req.selected_columns)}"
                logger_api.info(f"SEMI_AUTO: Further filtered based on {len(req.selected_columns)} selected columns. Remaining tables with columns: {len(schema_to_use)}")

            # 3. --- Final check for SEMI_AUTO: Ensure some schema remains AFTER all filtering ---
            if not schema_to_use:
                logger_api.warning("SEMI_AUTO mode resulted in an empty schema after all filtering (tables and columns).")
                # Provide a more specific error message
                error_detail = "The combination of selected tables and columns resulted in an empty schema. Ensure selected columns exist within the selected tables."
                raise HTTPException(400, error_detail)
# --- END: Corrected SEMI_AUTO Logic Block ---

        else: # AUTO Mode (or default if mode is invalid)
            # ... (AUTO mode logic remains the same) ...
            schema_to_use = all_table_schemas
            if req.table_prefix and req.table_prefix.strip():
                 prefix = req.table_prefix.strip()
                 original_count = len(schema_to_use) # Get count before filtering
                 schema_to_use = [ts for ts in schema_to_use if ts.table_id.startswith(prefix)]
                 schema_source_description = f"tables matching prefix '{prefix}' ({len(schema_to_use)}/{original_count})"
                 logger_api.info(f"AUTO: Filtered schema from {original_count} to {len(schema_to_use)} tables using prefix '{prefix}'.")
                 if not schema_to_use:
                    raise HTTPException(404, f"No tables found in dataset {req.dataset_id} matching prefix '{prefix}'.")
            else:
                 logger_api.info(f"AUTO: Using schema for all {len(schema_to_use)} tables (no prefix).")

        # --- Continue with building schema_string, prompt_template, calling AI, etc. ---
        # ...

    # ... (Continue to schema string building) ...


        # --- START: Refactored Schema String Generation ---
        # 3. Build the schema string *from the determined schema_to_use list*
        # Make the table.column relationship explicit for the AI
        schema_lines = []
        for ts in schema_to_use:
            if ts.columns: # Only include tables that still have columns
                # Format: `column_name` (TYPE)
                column_details = []
                for c in ts.columns:
                    # Standardize common BQ types for AI prompt, can be simplified if needed
                    # e.g., INT64 -> INTEGER, FLOAT64 -> FLOAT
                    col_type_for_ai = c.type.upper()
                    if col_type_for_ai == "INT64": col_type_for_ai = "INTEGER"
                    if col_type_for_ai == "FLOAT64": col_type_for_ai = "FLOAT"
                    # You can add more mappings if desired (e.g. BIGNUMERIC -> NUMERIC)
                    column_details.append(f"`{c.name}` ({col_type_for_ai})")

                columns_str = ', '.join(column_details)
                schema_lines.append(f"- Table `{ts.table_id}` (Columns: {columns_str})")
            else:
                logger_api.debug(f"Schema generation: Table '{ts.table_id}' has no columns after filtering, skipping for prompt.")


        schema_string = "\n".join(schema_lines).strip()
        # --- END: Refactored Schema String Generation ---

        full_dataset_id_str = f"`{req.dataset_id}`" # Use backticks for dataset ID too

        if not schema_string: # Should be caught earlier, but double-check
            logger_api.error("Schema string is empty before sending to AI. This should not happen.")
            raise HTTPException(500, "Internal error: Failed to prepare schema for AI.")


        # 4. Construct the AI Prompt (using the potentially filtered schema)
                # 4. Construct the AI Prompt (using the refined schema string)
        # --- START: Modified Prompt Template ---
        logger_api.info(f"OOOOOOOOOOOO {schema_string}")
        prompt_template = f"""You are an expert BigQuery SQL generator. Generate a *single*, valid, executable BigQuery SQL query based on the user request and the provided database schema subset.

**Database Schema (Dataset ID: {full_dataset_id_str}):**
Each table lists its columns with their data types in parentheses (e.g., `column_name` (TYPE)).
{schema_string}

**User Request:** "{req.prompt}"

**Instructions:**
1.  **Query Type:** Generate ONLY a valid BigQuery `SELECT` query. Do not generate DML or DDL.
2.  **STRICT Schema Adherence & Column Ownership (CRITICAL):**
    *   The *only* valid tables, columns, and data types you can use are those explicitly listed in the "Database Schema" section above.
    *   DO NOT invent table or column names. DO NOT assume a column exists in a table if it is not listed for that table in the "Database Schema".
    *   Each column name is specific to the table it's listed under.
    *   If the user's request seems to require data not present in the schema, state this in a `-- Cannot fulfill request:` comment.
3.  **Constructing Logical Joins (Multi-Table Awareness - VERY IMPORTANT):**
    *   Analyze the "User Request" to identify the key pieces of information needed (e.g., employee name, salary, department budget).
    *   Locate which tables contain this information based on the "Database Schema".
    *   **If the required information spans multiple tables that don't share an immediate common column, you MUST find an intermediate table that links them.** Look for paths: Table A joins to Table B on a common key, and Table B joins to Table C on another common key. This allows you to connect information from Table A to Table C.
    *   For this specific schema:
        *   `Master_Data_Employee_Records` (contains `Salary`) links to `Master_Data_Employee_Details` using `EmployeeID` (requires CAST).
        *   `Master_Data_Employee_Details` (contains `Department` name) links to `Master_Data_Status_Department` (contains `Budget`) using the `Department` column (string comparison).
        *   Therefore, to get Salary and Budget together, you NEED to join all three tables.
    *   Pay close attention to column data types when creating JOIN conditions. Use CAST functions (e.g., `CAST(t2.EmployeeID AS STRING)`) only when necessary to match types between join keys.
4.  **Mapping User Language to Schema:** Carefully map terms from the "User Request" to the actual column names and tables in the "Database Schema".
5.  **Table Qualification:** ALWAYS fully qualify table names: {full_dataset_id_str}.`YourTableName`. Use backticks `` ` ``.
6.  **Column Qualification:** Use table aliases (e.g., `t1`, `t2`, `t3`) and qualify ALL column names. Ensure qualified columns correctly reference their owning table alias.
7.  **Syntax:** Use correct BigQuery Standard SQL syntax.
8.  **Assumptions:** Make reasonable assumptions ONLY if inferable from the provided schema and request. If a logical join path *cannot* be constructed between the necessary tables using the provided schema, return ONLY a SQL comment explaining why. Do NOT ask clarifying questions.
9.  **Output:** Respond with *only* the raw SQL query text. No explanations, no markdown ```sql ... ``` blocks.

Generated BigQuery SQL Query:
"""
        # --- END: Modified Prompt Template ---

        logger_api.debug(f"Gemini Prompt (Mode: {req.ai_mode}, Schema Source: {schema_source_description}):\n{schema_string[:500]}...") # Log schema sample

        # ... (rest of the function: call Gemini, process response) ...

        # 5. Call Gemini API (Unchanged)
        model = genai.GenerativeModel('gemini-1.5-flash-latest')
        response = await model.generate_content_async(
             prompt_template,
             generation_config=genai.types.GenerationConfig(temperature=0.15), # Slightly higher temp
             request_options={'timeout': GEMINI_REQUEST_TIMEOUT}
        )

        # 6. Process Response (Unchanged)
        logger_api.debug(f"Gemini Raw Response Candidates: {response.candidates}")
        generated_sql = ""
        if response.candidates and response.candidates[0].content and response.candidates[0].content.parts:
             generated_sql = "".join(part.text for part in response.candidates[0].content.parts if hasattr(part, 'text')).strip()
        else:
             generated_sql = response.text.strip() if hasattr(response, 'text') else ""

        logger_api.debug(f"Gemini SQL (raw joined): {generated_sql[:500]}...")

        # Cleaning
        if generated_sql.startswith("```sql"): generated_sql = generated_sql[len("```sql"):].strip()
        if generated_sql.endswith("```"): generated_sql = generated_sql[:-len("```")].strip()
        generated_sql = re.sub(r"^\s*--.*?\n", "", generated_sql, flags=re.MULTILINE).strip()
        generated_sql = re.sub(r"\n--.*?$", "", generated_sql, flags=re.MULTILINE).strip()

        if not generated_sql:
             logger_api.warning("Gemini returned an empty response after cleaning.")
             return NLQueryResponse(error="AI failed to generate a query. Please try rephrasing.")
        if "-- Cannot fulfill request" in generated_sql:
             logger_api.warning(f"Gemini indicated request cannot be fulfilled: {generated_sql}")
             return NLQueryResponse(error=f"AI could not generate query: {generated_sql.strip('-- ')}")
        if not generated_sql.lower().lstrip().startswith("select"):
             logger_api.warning(f"Generated text does not start with SELECT: {generated_sql[:100]}...")
             # Consider returning an error if strict SELECT is required
             # return NLQueryResponse(error=f"AI did not generate a valid SELECT query. Output: {generated_sql[:100]}...")

        logger_api.info(f"NL-to-SQL successful (Filtered Schema: {'Yes' if req.table_prefix else 'No'}). Generated SQL: {generated_sql[:100]}...")
        return NLQueryResponse(generated_sql=generated_sql)

    except HTTPException as http_exc: # Re-raise user-facing errors (like 404s from filtering)
         raise http_exc
    except DeadlineExceeded: logger_api.error("Gemini API call timed out."); raise HTTPException(504, "NL-to-SQL generation timed out.")
    except GoogleAPICallError as e: logger_api.error(f"Gemini API call error: {e}"); raise HTTPException(502, f"Error communicating with AI service: {str(e)}")
    except Exception as e: logger_api.error(f"Error during NL-to-SQL generation: {e}", exc_info=True); raise HTTPException(500, f"Failed to generate SQL: {str(e)}")


# --- Other Endpoints ---
# (Keep /jobs, /tables, /table-data, /jobs/{job_id}, /jobs/{job_id}/results, /api/upload-url, /api/trigger-etl, /api/health, / EXACTLY as they were)
@bq_router.post("/jobs", response_model=JobSubmitResponse, status_code=202)
async def submit_bigquery_job(
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
async def list_bigquery_tables(
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
async def get_table_data(
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
async def get_bigquery_job_status(
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
async def get_bigquery_job_results(
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
async def get_upload_url(
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

@app.post("/api/trigger-etl", tags=["ETL"], dependencies=[Depends(verify_token)])
async def trigger_etl(
    payload: ETLRequest,
    request: Request, # Keep if needed for client_ip logging
    publisher: pubsub_v1.PublisherClient = Depends(get_pubsub_publisher), # Inject publisher
    topic_path: str = Depends(get_pubsub_topic_path) # Inject topic path
):
    # logger_api.info(f"[DEBUG] /api/trigger-etl received payload: {payload.dict()}")
    # if not api_publisher or not api_topic_path: logger_api.error("Cannot trigger ETL: Publisher client not available."); raise HTTPException(503, "Messaging service unavailable")
    #if not payload.object_name or not payload.object_name.startswith("uploads/"): logger_api.warning(f"Invalid object_name received for ETL trigger: {payload.object_name}"); raise HTTPException(400, "Invalid object_name provided. Must start with 'uploads/'.")
    # safe_dataset_prefix = re.sub(r"[^a-zA-Z0-9_]", "_", payload.target_dataset_id)
    # if not payload.object_name or not payload.target_dataset_id:
    #      logger_api.warning(f"Invalid payload content for ETL trigger: object_name or target_dataset_id is empty/null. Payload: {payload.dict()}")
    #      raise HTTPException(400, "Invalid payload content: object_name and target_dataset_id must not be empty.")
    # client_ip = request.client.host if request.client else "unknown"
    # logger_api.info(f"Triggering ETL for object: {payload.object_name} (requested by {client_ip})")
    """Triggers the ETL process by publishing a message to Pub/Sub."""
    if not payload.object_name or not payload.target_dataset_id: raise HTTPException(400, "Invalid payload.")
    client_ip = request.client.host if request.client else "unknown"
    logger_api.info(f"Triggering ETL for: {payload.object_name} from {client_ip}")
    try:
        # message_data = { "object_name": payload.object_name, "target_dataset_id": payload.target_dataset_id }
        # data = json.dumps(message_data).encode("utf-8") # Sends the JSON string encoded
        # future = api_publisher.publish(api_topic_path, data=data)
        message_data = payload.dict()
        data = json.dumps(message_data).encode("utf-8")
        future = publisher.publish(topic_path, data=data) # Use injected client & path
        def pubsub_callback(f):
             try: message_id = f.result(); logger_api.info(f"Pub/Sub message published successfully for {payload.object_name}. Message ID: {message_id}")
             except Exception as pub_e: logger_api.error(f"Failed to publish Pub/Sub message for {payload.object_name}: {pub_e}", exc_info=True)
        future.add_done_callback(pubsub_callback)
        logger_api.info(f"ETL job queued successfully for {payload.object_name}")
        return {"status": "queued", "object_name": payload.object_name}
    except TimeoutError:
        logger_api.error(f"Timeout publishing ETL trigger for {payload.object_name} to Pub/Sub.")
        raise HTTPException(status.HTTP_504_GATEWAY_TIMEOUT, detail="Timeout communicating with messaging service.")
    except GoogleAPICallError as e: logger_api.error(f"PubSub API Error trigger ETL: {e}"); raise HTTPException(502, f"PubSub API Error: {str(e)}")
    except Exception as e: logger_api.error(f"Trigger ETL error: {e}"); raise HTTPException(500, f"Could not trigger ETL: {str(e)}")
    # except GoogleAPICallError as e: logger_api.error(f"Google API Call Error publishing ETL trigger for {payload.object_name} to Pub/Sub: {e}", exc_info=True); raise HTTPException(502, f"Error communicating with Pub/Sub API: {str(e)}")
    # except Exception as e: logger_api.error(f"Error publishing ETL trigger for {payload.object_name} to Pub/Sub: {e}", exc_info=True); raise HTTPException(500, f"Could not trigger ETL: {str(e)}")




# --- Optional Health Check ---
@app.get("/api/health", tags=["Health"])
async def health_check( # Inject clients to check their status
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
async def summarize_results(req: AISummaryRequest):
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
        model = genai.GenerativeModel('gemini-1.5-flash-latest') # Or another suitable model
        response = await model.generate_content_async(
             summary_prompt,
             # Use a moderate temperature for informative summary
             generation_config=genai.types.GenerationConfig(temperature=0.4),
             # Timeout might need adjustment depending on complexity
             request_options={'timeout': GEMINI_REQUEST_TIMEOUT}
         )

        # --- Process Response ---
        logger_api.debug(f"Gemini Raw Summary Response: {response.text}")
        generated_summary = response.text.strip() if hasattr(response, 'text') else ""

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
async def suggest_visualization(req: SuggestVizRequest):
    """Suggests appropriate visualizations based on query results schema and context."""

    if not GEMINI_API_KEY:
        logger_api.warning("Visualization suggestion requested but Gemini API key not set.")
        return SuggestVizResponse(suggestions=[], error="AI suggestion service not configured.")

    if not req.schema_:
        raise HTTPException(status_code=400, detail="Schema information is required for suggestions.")

    logger_api.info(f"Received visualization suggestion request. Query: {req.query_sql[:50] if req.query_sql else 'N/A'}, Prompt: {req.prompt[:50] if req.prompt else 'N/A'}")

    try:  # Outer try starts here
        # Prepare schema string for the prompt
        schema_desc = "\n".join([f"- Column '{s.get('name', 'Unknown')}' (Type: {s.get('type', '?')})" for s in req.schema_])

        # Prepare sample string
        sample_str = ""
        if req.result_sample:
            try:
                sample_str = "\nResult Sample (first few rows):\n"
                if isinstance(req.result_sample, list) and len(req.result_sample) > 0 and isinstance(req.result_sample[0], dict):
                    headers = list(req.result_sample[0].keys())
                    sample_str += "| " + " | ".join(headers) + " |\n"
                    sample_str += "|-" + "-|-".join(['-' * len(h) for h in headers]) + "-|\n"
                    for row in req.result_sample:
                        # Ensure values are strings before joining
                        values = [str(row.get(h, '')) for h in headers] # Calculate values list
                        sample_str += f"| {' | '.join(values)} |\n" # Join AFTER converting
                else:
                    sample_str += "(Sample not available or in unexpected format)\n"

            except Exception as e_sample:
                logger_api.warning(f"Could not format result sample for prompt: {e_sample}")
                sample_str = "\n(Could not process sample data)\n"
        # --- END OF SAMPLE STRING BLOCK ---


        # --- CORRECTED: Construct context parts separately to avoid f-string backslash error ---
        prompt_info = f"The query was generated from the natural language prompt: '{req.prompt}'" if req.prompt else ""
        # Ensure newline after the closing backticks for clarity
        sql_query_info = f"The SQL query executed was: ```sql\n{req.query_sql}\n```" if req.query_sql else ""
        # --- END OF CORRECTION ---


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
        3.  `y_axis_columns`: A list containing ONE or MORE EXACT column names from the schema for the Y-axis (or values for pie/scatter). Use only numeric columns for Y-axis values (e.g., INTEGER, FLOAT, NUMERIC).
        4.  `rationale`: A SHORT explanation (max 1-2 sentences) why this chart is suitable.

        Output ONLY a valid JSON object containing a single key "suggestions" which is a list of suggestion objects (with keys chart_type, x_axis_column, y_axis_columns, rationale). Do not include any other text, explanations, or markdown.

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
        """ # <-- This is the end of the f-string block, error likely pointed near here
        logger_api.debug(f"Gemini Viz Suggestion Prompt:\n{prompt_context[:500]}...")

        model = genai.GenerativeModel(
             'gemini-1.5-flash-latest',
             generation_config=genai.types.GenerationConfig(
                response_mime_type="application/json", # Explicitly request JSON
                temperature=0.2
            )
         )
        response = await model.generate_content_async(
             prompt_context,
             # Set a reasonable timeout, maybe shorter than regular queries
            request_options={'timeout': int(os.getenv("GEMINI_TIMEOUT_SECONDS", 120)) // 2}
         )

        logger_api.debug(f"Gemini Raw Viz Suggestion Response Text: {response.text}")

        # Inner try block for JSON parsing
        import json # Ensure json is imported
        try:
            # Clean potential markdown ```json ... ``` artifacts if the model doesn't strictly adhere
            cleaned_response = response.text.strip()
            if cleaned_response.startswith("```json"):
                cleaned_response = cleaned_response[len("```json"):].strip()
            if cleaned_response.endswith("```"):
                cleaned_response = cleaned_response[:-len("```")].strip()

            # Check for empty response after cleaning
            if not cleaned_response:
                 logger_api.warning("Gemini returned an empty string after cleaning.")
                 return SuggestVizResponse(suggestions=[], error="AI returned an empty response.")

            suggestions_data = json.loads(cleaned_response)

            # Validate the received structure
            if isinstance(suggestions_data, dict) and 'suggestions' in suggestions_data and isinstance(suggestions_data['suggestions'], list):
                validated_suggestions = []
                schema_column_names = {s.get('name') for s in req.schema_ if s.get('name')} # Get valid column names

                for sugg_raw in suggestions_data['suggestions']:
                    # Validate individual suggestion structure and types
                    if isinstance(sugg_raw, dict) and \
                       all(k in sugg_raw for k in ['chart_type', 'x_axis_column', 'y_axis_columns', 'rationale']) and \
                       isinstance(sugg_raw['y_axis_columns'], list) and \
                       isinstance(sugg_raw['x_axis_column'], str) and \
                       sugg_raw['x_axis_column'] in schema_column_names and \
                       all(isinstance(yc, str) and yc in schema_column_names for yc in sugg_raw['y_axis_columns']) and \
                       len(sugg_raw['y_axis_columns']) > 0:
                         # Optional: Further validation on chart_type enum?

                         validated_suggestions.append(VizSuggestion(
                            chart_type=sugg_raw['chart_type'],
                            x_axis_column=sugg_raw['x_axis_column'],
                            y_axis_columns=sugg_raw['y_axis_columns'],
                            rationale=sugg_raw.get('rationale', 'AI Suggestion.') # Provide default rationale
                         ))
                    else:
                         logger_api.warning(f"Skipping invalid or incomplete suggestion format from AI: {sugg_raw}")

                logger_api.info(f"Gemini generated {len(validated_suggestions)} valid visualization suggestions.")
                return SuggestVizResponse(suggestions=validated_suggestions)
            else:
                logger_api.error(f"Gemini response JSON root structure is invalid: {suggestions_data}")
                return SuggestVizResponse(suggestions=[], error="AI returned suggestions in an unexpected format.")

        except json.JSONDecodeError as json_err:
             logger_api.error(f"Failed to parse JSON response from Gemini: {json_err}\nResponse Text: {response.text}")
             return SuggestVizResponse(suggestions=[], error="AI response was not valid JSON.")
        except Exception as e_parse: # Catch validation or Pydantic errors
             logger_api.error(f"Error processing Gemini suggestions: {e_parse}", exc_info=True)
             return SuggestVizResponse(suggestions=[], error=f"Error processing AI suggestions: {str(e_parse)}")

    # --- Outer try block exceptions ---
    except DeadlineExceeded:
        logger_api.error("Gemini API call for suggestions timed out.")
        raise HTTPException(status_code=504, detail="AI suggestion generation timed out.")
    except GoogleAPICallError as e:
        logger_api.error(f"Gemini API call error for suggestions: {e}", exc_info=True)
        raise HTTPException(status_code=502, detail=f"Error communicating with AI service for suggestions: {str(e)}")
    except Exception as e: # Generic catch for the outer try
        logger_api.error(f"Unexpected error during visualization suggestion generation: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to generate suggestions: {str(e)}")

# --- Make sure you include the router in your main app ---
# Example: app.include_router(bq_router) should be present somewhere
# --- New Endpoint to List Datasets ---



@bq_router.get("/datasets", response_model=DatasetListResponse, tags=["BigQuery"])
async def list_bigquery_datasets(
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
        user_role = await get_user_role(user_uid)
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
        else:
            logger_api.info(f"User {user_uid} is not admin. Fetching accessible datasets from Firestore.")
            accessible_dataset_ids = await get_user_accessible_datasets(user_uid)

            if accessible_dataset_ids is None:
                # Error fetching from Firestore or user not found
                logger_api.warning(f"Could not determine accessible datasets for user {user_uid} (Firestore error or user not found). Returning empty list.")
                final_datasets_list = []
            elif not accessible_dataset_ids:
                # User found, but no datasets assigned (empty list)
                logger_api.info(f"User {user_uid} has no datasets assigned in Firestore. Returning empty list.")
                final_datasets_list = []
            else:
                # User found with specific datasets assigned - Filter the BQ list
                logger_api.info(f"User {user_uid} has access to {len(accessible_dataset_ids)} specific datasets. Filtering BQ list.")
                allowed_set = set(accessible_dataset_ids) # Use a set for faster lookups
                final_datasets_list = [
                    ds for ds in all_datasets_from_bq if ds.datasetId in allowed_set
                ]
                logger_api.info(f"Filtered list size for user {user_uid}: {len(final_datasets_list)}")

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
    status_code=status.HTTP_201_CREATED, # Use status module
    tags=["BigQuery"],
    summary="Create a new BigQuery Dataset (Admin Only)",
    description="Creates a new BigQuery dataset. Requires administrator privileges.",
    # +++ Apply the RBAC dependency +++
    dependencies=[Depends(require_admin)]
)
async def create_bigquery_dataset(
    req: CreateDatasetRequest,
    bq_client: bigquery.Client = Depends(get_bigquery_client), # Inject client
    # user_data: dict = Depends(require_admin) # Already in router deps
):
    """
    Creates a new BigQuery dataset. (Admin Only)
    Requires the requesting user to have the 'admin' role stored in Firestore.
    """
    # if not api_bigquery_client:
    #     logger_api.error("Cannot create dataset: BigQuery client not available.")
    #     raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="BigQuery service unavailable.")
    if not API_GCP_PROJECT:
        logger_api.error("Cannot create dataset: GCP_PROJECT not configured.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Server configuration error (missing project ID).")

    logger_api.info(f"Admin request received to create dataset '{req.dataset_id}' in location '{req.location or DEFAULT_BQ_LOCATION}'")

    try:
        dataset_ref = bigquery.DatasetReference(API_GCP_PROJECT, req.dataset_id)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = req.location or DEFAULT_BQ_LOCATION
        if req.description: dataset.description = req.description
        if req.labels: dataset.labels = req.labels

        # created_dataset = api_bigquery_client.create_dataset(dataset, timeout=30, exists_ok=False)
        created_dataset = bq_client.create_dataset(dataset, timeout=30, exists_ok=False)
        logger_api.info(f"Admin successfully created dataset: {created_dataset.full_dataset_id}")

        return DatasetCreatedResponse(
            project_id=created_dataset.project,
            dataset_id=created_dataset.dataset_id,
            location=created_dataset.location,
            description=created_dataset.description,
            labels=created_dataset.labels,
        )
    # Keep existing specific error handling
    except BadRequest as e:
        logger_api.warning(f"Bad request creating dataset '{req.dataset_id}': {e}", exc_info=False)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid request: {str(e)}")
    except Conflict: # Catch the specific Conflict exception
        logger_api.warning(f"Dataset '{req.dataset_id}' already exists in project '{API_GCP_PROJECT}'.")
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Dataset '{req.dataset_id}' already exists.")
    except GoogleAPICallError as e:
        logger_api.error(f"Google API Call Error creating dataset '{req.dataset_id}': {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=f"Error communicating with BigQuery API: {str(e)}")
    except Exception as e:
        logger_api.error(f"Unexpected error creating dataset '{req.dataset_id}': {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected server error occurred: {str(e)}")


@bq_router.delete(
    "/datasets/{dataset_id}",
    status_code=status.HTTP_204_NO_CONTENT, # Standard for successful DELETE
    tags=["BigQuery"],
    summary="Delete a BigQuery Dataset (Admin Only)",
    description="Permanently deletes a BigQuery dataset and all its contents (tables, views). Requires administrator privileges.",
    dependencies=[Depends(require_admin)], # Apply RBAC
    responses={
        204: {"description": "Dataset deleted successfully"},
        403: {"description": "Permission denied: Admin role required"},
        404: {"description": "Dataset not found"},
        500: {"description": "Internal server error"},
        502: {"description": "Error communicating with BigQuery API"},
        503: {"description": "BigQuery service unavailable"}
    }
)
async def delete_bigquery_dataset(
    dataset_id: str = Path(..., description="The ID of the dataset to delete.", example="my_team_dataset_to_delete"),
    admin_user: dict = Depends(require_admin), # Get admin user data for logging if needed
    bq_client: bigquery.Client = Depends(get_bigquery_client) # Inject client
):
    """
    Deletes an existing BigQuery dataset, including all tables within it.
    Requires the requesting user to have the 'admin' role stored in Firestore.
    """
    admin_uid = admin_user.get("uid", "unknown_admin") # Get admin UID for logging
    # if not api_bigquery_client:
    #     logger_api.error(f"Admin {admin_uid}: Cannot delete dataset '{dataset_id}': BigQuery client not available.")
    #     raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="BigQuery service unavailable.")
    if not API_GCP_PROJECT:
        logger_api.error(f"Admin {admin_uid}: Cannot delete dataset '{dataset_id}': GCP_PROJECT not configured.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Server configuration error (missing project ID).")

    full_dataset_id = f"{API_GCP_PROJECT}.{dataset_id}"
    logger_api.warning(f"ADMIN ACTION by UID {admin_uid}: Attempting to DELETE dataset: {full_dataset_id} and all its contents!")

    try:
        dataset_ref = bigquery.DatasetReference(API_GCP_PROJECT, dataset_id)

        # Delete the dataset.
        # delete_contents=True: Deletes tables within the dataset. If False, deletion fails if dataset is not empty.
        # not_found_ok=False: Raises NotFound exception if the dataset doesn't exist.
        # api_bigquery_client.delete_dataset(
        #     dataset_ref, delete_contents=True, not_found_ok=False
        # )
        bq_client.delete_dataset(
            dataset_ref, delete_contents=True, not_found_ok=False
        )

        logger_api.info(f"Admin {admin_uid}: Successfully deleted dataset: {full_dataset_id}")
        # Return HTTP 204 No Content on success, no response body needed.
        # Note: Returning Response(status_code=204) might be more explicit for some frameworks
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    except NotFound:
        logger_api.warning(f"Admin {admin_uid}: Dataset not found during delete attempt: {full_dataset_id}")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Dataset '{dataset_id}' not found.")
    except GoogleAPICallError as e:
        # Catch specific BQ API errors (e.g., permission issues on the *service account*)
        logger_api.error(f"Admin {admin_uid}: Google API Call Error deleting dataset '{full_dataset_id}': {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=f"Error communicating with BigQuery API during deletion: {str(e)}")
    except Exception as e:
        # Catch any other unexpected errors
        logger_api.error(f"Admin {admin_uid}: Unexpected error deleting dataset '{full_dataset_id}': {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected server error occurred during deletion: {str(e)}")

# +++ END NEW ENDPOINT: Delete Dataset +++

@bq_router.post("/suggest-prompt", response_model=PromptSuggestionResponse)
async def suggest_prompt_completion(req: PromptSuggestionRequest):
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
        model = genai.GenerativeModel(
            'gemini-1.5-flash-latest',
             generation_config=genai.types.GenerationConfig(
                 response_mime_type="application/json", # Request JSON output
                 temperature=0.5 # Allow some creativity in suggestions
             )
        )

        response = await model.generate_content_async(
             suggestion_ai_prompt,
             request_options={'timeout': GEMINI_REQUEST_TIMEOUT // 2} # Use shorter timeout for suggestions
         )

        # --- Process Response ---
        logger_api.debug(f"Gemini Raw Suggestion Response: {response.text}")

        suggestions = []
        error_msg = None
        try:
            # Clean potential markdown ```json ... ``` artifacts
            cleaned_response = response.text.strip()
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
             logger_api.error(f"Failed to parse JSON response from Gemini for suggestions: {json_err}\nResponse Text: {response.text}")
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
async def delete_bigquery_table(
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

# --- Include Routers ---
app.include_router(bq_router)
app.include_router(chat_router)
app.include_router(export_router) 
app.include_router(user_profile_router) # +++ Include the new user profile router +++
# --- Uvicorn Runner ---
if __name__ == "__main__":
    import uvicorn
    api_host = os.getenv("API_HOST", "127.0.0.1")
    api_port = int(os.getenv("API_PORT", 8000))
    logger_api.info(f"Starting Uvicorn server on http://{api_host}:{api_port}")
    uvicorn.run("main:app", host=api_host, port=api_port, reload=False)
