# backend/routers/export.py

import os
import logging
import re
import io
import traceback # Ensure traceback is imported for detailed error logging
from typing import List, Dict, Any, Optional, Set
import pandas as pd
from dotenv import load_dotenv
from fastapi import APIRouter, Depends, HTTPException, Response, status # Import status
from pydantic import BaseModel, Field
from google.cloud import bigquery # Keep for type hints
from google.api_core.exceptions import GoogleAPICallError, NotFound, Forbidden

# --- Import Config Variables ---
# Import only specific variables needed from config
from config import DEFAULT_BQ_LOCATION, DEFAULT_JOB_TIMEOUT_SECONDS

# --- Import Dependencies ---
from auth import verify_token # For authentication
from dependencies.client_deps import get_bigquery_client # Import client dependency getter

# --- Get Logger for this module ---
logger_export = logging.getLogger(__name__) # Get a logger specific to this module

load_dotenv() # Load .env if used for local development config

# --- Configuration ---
MAX_SOURCE_TABLE_ROWS = int(os.getenv("EXPORT_MAX_SOURCE_ROWS", 5000)) # Example: make configurable

# --- Router Setup ---
export_router = APIRouter(
    prefix="/api/export",
    tags=["Export"],
    dependencies=[Depends(verify_token)] # Apply authentication to all export routes
)

# --- Pydantic Models ---
class QueryExportRequest(BaseModel):
    sql: str = Field(..., description="The SQL query that was executed.")
    job_id: str = Field(..., description="The BigQuery Job ID for the executed query.")
    location: Optional[str] = Field(DEFAULT_BQ_LOCATION, description="The location where the job ran.")

# --- Helper Functions ---

def extract_fully_qualified_tables(sql: str) -> List[str]:
    """
    Extracts fully qualified table names (project.dataset.table) from SQL.
    Handles backticks and preserves original casing.
    """
    # Regex to find fully qualified tables (project.dataset.table or dataset.table) after FROM or JOIN
    # Handles optional backticks around the full name or individual parts
    regex = r"""
        (?:FROM|JOIN)\s+             # Match FROM or JOIN followed by space
        `?                           # Optional starting backtick for the whole name
        (                            # Start capturing group 1 (full qualified name)
          (?:                        # Start non-capturing group for project/dataset parts
            `?                       # Optional backtick for part
            [a-zA-Z0-9_.-]+          # Match project/dataset name characters
            `?                       # Optional closing backtick for part
            \.                       # Match the dot separator
          )+                         # Match one or more project/dataset parts (e.g., proj.dataset.)
          `?                         # Optional backtick for table name part
          [a-zA-Z0-9_-]+             # Match table name characters
          `?                         # Optional closing backtick for table name part
        )                            # End capturing group 1
        `?                           # Optional closing backtick for the whole name
    """
    matches = re.finditer(regex, sql, re.VERBOSE | re.IGNORECASE | re.MULTILINE)
    tables_dict: Dict[str, str] = {} # Use dict to store unique tables (lowercase key, original case value)
    for match in matches:
        table_name = match.group(1).replace('`', '') # Remove all backticks
        # Ensure it looks like at least dataset.table
        if table_name.count('.') >= 1:
            lower_case_key = table_name.lower()
            # Store the first occurrence with original casing
            if lower_case_key not in tables_dict:
                tables_dict[lower_case_key] = table_name
    original_case_tables = list(tables_dict.values())
    logger_export.info(f"Extracted source tables (preserved case): {original_case_tables}")
    return original_case_tables

# --- MODIFIED HELPER to accept bq_client ---
async def fetch_bq_data_to_dataframe(
    query: str,
    location: Optional[str], # Location might be needed for the query job config
    bq_client: bigquery.Client # Accept the initialized client
) -> pd.DataFrame:
    """
    Fetches data using a query and returns a Pandas DataFrame using the provided client.
    Handles common errors and timeouts.
    """
    query_job = None # Define here for access in exception blocks
    # Use the timeout defined in config, defaulting if not set
    timeout_seconds = DEFAULT_JOB_TIMEOUT_SECONDS

    try:
        logger_export.info(f"Executing BQ query for export helper: {query[:150]}...")
        # Use the passed BigQuery client instance
        query_job_config = bigquery.QueryJobConfig() # Add job config if needed (e.g., query parameters)
        query_job = bq_client.query(query, location=location, job_config=query_job_config)
        logger_export.debug(f"Waiting for helper query job {query_job.job_id} (timeout: {timeout_seconds}s)...")

        # Use to_dataframe with timeout - this waits for the job to complete
        df = query_job.to_dataframe(timeout=timeout_seconds)

        logger_export.info(f"Helper fetched {len(df)} rows into DataFrame for job {query_job.job_id}.")
        return df

    except TimeoutError: # Catch specific timeout from to_dataframe(timeout=...)
        job_id_str = getattr(query_job, 'job_id', 'unknown')
        logger_export.error(f"Helper query job {job_id_str} timed out after {timeout_seconds} seconds.")
        # Return a DataFrame indicating the error for this specific source table
        return pd.DataFrame([{"error": f"Source table query timed out ({timeout_seconds}s)", "query": query}])
    except NotFound as e:
        logger_export.warning(f"Helper query error (NotFound): {e}. Query: {query[:150]}")
        return pd.DataFrame([{"error": f"Source table/resource not found: {e.message}", "query": query}])
    except Forbidden as e:
         logger_export.warning(f"Helper query error (Forbidden): {e}. Query: {query[:150]}")
         return pd.DataFrame([{"error": f"Permission denied for source table: {e.message}", "query": query}])
    except (GoogleAPICallError, ValueError, TypeError) as e: # Catch other potential API or data errors
        logger_export.error(f"Helper query error: {e}. Query: {query[:150]}", exc_info=True)
        return pd.DataFrame([{"error": f"Failed source data fetch: {type(e).__name__} - {str(e)}", "query": query}])
    except Exception as e: # Catch any other unexpected errors
        logger_export.error(f"Unexpected error in helper query: {e}. Query: {query[:150]}", exc_info=True)
        return pd.DataFrame([{"error": f"Unexpected error fetching source: {str(e)}", "query": query}])


def make_datetime_naive(df: pd.DataFrame) -> pd.DataFrame:
    """Converts timezone-aware datetime columns in a DataFrame to timezone-naive for Excel."""
    if df is None or df.empty:
        return df
    # Select columns that are explicitly timezone-aware
    tz_aware_cols = df.select_dtypes(include=['datetime64[ns, UTC]', 'datetimetz']).columns
    if not tz_aware_cols.empty:
        logger_export.debug(f"Found timezone-aware columns: {list(tz_aware_cols)}")
        for col in tz_aware_cols:
            try:
                # Double-check with attribute access before converting
                if getattr(df[col].dt, 'tz', None) is not None:
                    logger_export.debug(f"Converting column '{col}' to timezone-naive...")
                    # tz_convert(None) is generally preferred over tz_localize(None)
                    # It converts to UTC first if necessary, then removes tz info.
                    df[col] = df[col].dt.tz_convert(None)
            except Exception as e:
                logger_export.warning(f"Could not convert column '{col}' to naive datetime: {e}. Skipping conversion.")
    return df


# --- API Endpoint ---
@export_router.post("/query-to-excel",
                    response_class=Response, # Allows returning raw bytes
                    summary="Export Query Results to Excel",
                    description="Exports the results of a completed BigQuery job, the original SQL query, and a preview of source tables mentioned in the query to an Excel file with multiple sheets.",
                    responses={
                        200: {
                            "description": "Excel file generated successfully.",
                            "content": {"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {
                                "schema": {"type": "string", "format": "binary"}
                            }}
                        },
                        400: {"description": "Invalid request (e.g., Job not done, Job failed)"},
                        404: {"description": "Job ID not found."},
                        500: {"description": "Internal server error during export generation."},
                        503: {"description": "BigQuery service unavailable."},
                    })
async def export_query_to_excel(
    req: QueryExportRequest,
    bq_client: bigquery.Client = Depends(get_bigquery_client) # Inject BigQuery client
):
    """
    Exports the result of a BigQuery job, the original SQL, and previews
    of source tables to an Excel file.
    """
    logger_export.info(f"Received Excel export request for Job ID: {req.job_id}, SQL: {req.sql[:60]}...")

    # Use the injected BigQuery client
    if not bq_client: # Defensive check, although Depends should handle it
         logger_export.error("Export Endpoint: BigQuery client is None via dependency.")
         raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="BigQuery client dependency failed.")

    try:
        # 1. Fetch Job Results using injected client
        logger_export.info(f"Fetching results for job {req.job_id} in location {req.location}...")
        try:
            job = bq_client.get_job(req.job_id, location=req.location)
        except NotFound:
             logger_export.warning(f"Job ID {req.job_id} not found in location {req.location}.")
             raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Job ID '{req.job_id}' not found.")

        if job.state != 'DONE':
            logger_export.warning(f"Job {req.job_id} is not complete (State: {job.state}). Cannot export.")
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Job {req.job_id} not complete (State: {job.state}).")
        if job.error_result:
            err_msg = job.error_result.get('message', 'Unknown error')
            logger_export.warning(f"Job {req.job_id} failed: {err_msg}. Cannot export.")
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Job {req.job_id} failed: {err_msg}")

        results_df: pd.DataFrame
        if not job.destination:
            logger_export.info(f"Job {req.job_id} did not produce a destination table (e.g., DML). Creating info sheet.")
            results_df = pd.DataFrame([{"message": "Query did not produce a standard result table (e.g., DML or script)."}])
        else:
             # Fetch all rows from the destination table for the export
             # Consider adding limits or warnings for extremely large result sets if necessary
            logger_export.info(f"Fetching ALL rows from destination table for job {req.job_id}...")
            rows_iterator = bq_client.list_rows(job.destination)
            results_df = rows_iterator.to_dataframe()
            logger_export.info(f"Fetched {len(results_df)} result rows for job {req.job_id}.")

        # 2. Extract Source Tables from the original SQL
        source_tables = extract_fully_qualified_tables(req.sql)

        # 3. Fetch Source Table Previews (pass injected client to helper)
        source_data_frames: Dict[str, pd.DataFrame] = {}
        processed_sheet_names: Set[str] = set() # To handle potential duplicate base names

        for table_fqn in source_tables:
            # Skip INFORMATION_SCHEMA tables as they are usually not needed for data context
            if 'information_schema' in table_fqn.lower():
                 logger_export.info(f"Skipping fetch for INFORMATION_SCHEMA table: {table_fqn}")
                 continue

            logger_export.info(f"Fetching preview for source table: {table_fqn} (Limit: {MAX_SOURCE_TABLE_ROWS})")
            # Construct the preview query
            query = f"SELECT * FROM `{table_fqn}` LIMIT {MAX_SOURCE_TABLE_ROWS}"
            try:
                # Pass the injected bq_client to the helper function
                df = await fetch_bq_data_to_dataframe(query, req.location, bq_client)

                # Sanitize sheet name (Excel limits sheet names to 31 chars and restricts chars)
                base_table_name = table_fqn.split('.')[-1] # Get last part
                sanitized_base_name = re.sub(r'[\\/*?:\[\]]', '_', base_table_name)[:25] # Limit length early
                sheet_name = sanitized_base_name
                count = 1
                # Ensure unique sheet name (case-insensitive check)
                while sheet_name.lower() in (name.lower() for name in processed_sheet_names):
                    sheet_name = f"{sanitized_base_name[:28-len(str(count))]}_{count}" # Adjust length limit
                    count += 1
                    if count > 99: # Safety break
                         sheet_name = f"Source_{count}" # Fallback if name conflict persists

                # Check if the DataFrame contains only an error message from the helper
                is_error_df = 'error' in df.columns and 'query' in df.columns and len(df) == 1
                final_sheet_name = f"Error_{sheet_name}" if is_error_df else f"Source_{sheet_name}"

                # Ensure final sheet name is unique too (should be due to prefix/count, but double check)
                final_unique_sheet_name = final_sheet_name
                final_count = 1
                while final_unique_sheet_name.lower() in (name.lower() for name in processed_sheet_names):
                    final_unique_sheet_name = f"{final_sheet_name[:28-len(str(final_count))]}_{final_count}"
                    final_count += 1

                source_data_frames[final_unique_sheet_name] = df
                processed_sheet_names.add(final_unique_sheet_name)

            except Exception as e:
                 # Catch unexpected errors within the loop itself
                 logger_export.error(f"Unexpected error during source table fetch loop for {table_fqn}: {e}", exc_info=True)
                 error_sheet_name = f"LoopError_{table_fqn.split('.')[-1][:18]}"
                 # Ensure unique error sheet name
                 err_count = 1
                 while error_sheet_name.lower() in (name.lower() for name in processed_sheet_names):
                    error_sheet_name = f"LoopError_{table_fqn.split('.')[-1][:18]}_{err_count}"
                    err_count += 1
                 source_data_frames[error_sheet_name] = pd.DataFrame([{"error": f"Unexpected Error processing source table: {str(e)}"}])
                 processed_sheet_names.add(error_sheet_name)

        # 4. Create Excel File in Memory using openpyxl engine
        logger_export.info("Creating Excel file in memory...")
        excel_buffer = io.BytesIO()
        try:
            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                # Write Results sheet (make datetimes naive first)
                logger_export.debug("Writing 'Results' sheet...")
                results_df_naive = make_datetime_naive(results_df.copy())
                results_df_naive.to_excel(writer, sheet_name='Results', index=False)

                # Write Query sheet
                logger_export.debug("Writing 'Query' sheet...")
                pd.DataFrame({'SQL Query': [req.sql]}).to_excel(writer, sheet_name='Query', index=False)

                # Write Source Table Preview sheets (make datetimes naive first)
                for final_sheet_name, df_to_write in source_data_frames.items():
                    logger_export.debug(f"Writing '{final_sheet_name}' sheet...")
                    df_naive = make_datetime_naive(df_to_write.copy())
                    df_naive.to_excel(writer, sheet_name=final_sheet_name, index=False)

            excel_buffer.seek(0) # Rewind buffer to the beginning
        except Exception as excel_error:
            logger_export.error(f"Failed to write data to Excel buffer: {excel_error}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Failed to generate Excel file content: {str(excel_error)}")

        # 5. Prepare and Return the Response
        filename = f"query_export_{req.job_id[:8]}.xlsx" # Generate a filename
        headers = {
            'Content-Disposition': f'attachment; filename="{filename}"',
            'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        }
        logger_export.info(f"Successfully generated Excel export: {filename}")
        return Response(content=excel_buffer.getvalue(), headers=headers, media_type=headers['Content-Type'])

    # --- Exception Handling for the main endpoint ---
    except HTTPException as http_exc:
        # Re-raise HTTPExceptions (like 400, 404, 503 from checks)
        raise http_exc
    except GoogleAPICallError as e:
        # Catch Google API errors not caught in helpers (e.g., initial get_job)
        logger_export.error(f"Google API Error during export for job {req.job_id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=f"Error communicating with Google Cloud: {str(e)}")
    except Exception as e:
        # Catch any other unexpected errors during the process
        logger_export.error(f"Unexpected error during Excel export for job {req.job_id}: {e}", exc_info=True)
        logger_export.error(traceback.format_exc()) # Log full traceback
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to generate export file due to an unexpected error.")