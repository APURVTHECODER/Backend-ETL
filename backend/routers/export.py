# routers/export.py

import os
import logging
import re
import io
from typing import List, Dict, Any, Optional, Set # Added Set
import pandas as pd
import config # Keep this import
from dotenv import load_dotenv
from fastapi import APIRouter, Depends, HTTPException, Response
from pydantic import BaseModel, Field
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPICallError, NotFound, Forbidden

# --- Imports from config ---
from config import api_bigquery_client, DEFAULT_BQ_LOCATION, logger_config as logger_api

# --- Authentication ---
from auth import verify_token

load_dotenv()

# --- Configuration ---
MAX_SOURCE_TABLE_ROWS = 5000

# --- Router Setup ---
export_router = APIRouter(
    prefix="/api/export",
    tags=["Export"],
    dependencies=[Depends(verify_token)]
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
    regex = r"""
        (?:FROM|JOIN)\s+
        `?((?:`?[a-zA-Z0-9_.-]+`?\.)+(?:`?[a-zA-Z0-9_-]+`?))`?
    """
    matches = re.finditer(regex, sql, re.VERBOSE | re.IGNORECASE | re.MULTILINE)
    tables_dict: Dict[str, str] = {}
    for match in matches:
        table_name = match.group(1).replace('`', '')
        if table_name.count('.') >= 2:
            lower_case_key = table_name.lower()
            if lower_case_key not in tables_dict:
                tables_dict[lower_case_key] = table_name
    original_case_tables = list(tables_dict.values())
    logger_api.info(f"Extracted source tables (preserved case): {original_case_tables}")
    return original_case_tables

async def fetch_bq_data_to_dataframe(query: str, location: str) -> pd.DataFrame:
    """Fetches data using a query and returns a Pandas DataFrame. Handles common errors."""
    if not api_bigquery_client:
        logger_api.error("Export Error: BigQuery client is None during request handling.")
        raise HTTPException(status_code=503, detail="BigQuery client not available (initialization issue).")

    query_job = None # Define query_job here to access in except TimeoutError
    try:
        logger_api.info(f"Executing BQ query for export: {query[:100]}...")
        query_job = api_bigquery_client.query(query, location=location)
        timeout_seconds = getattr(config, 'DEFAULT_JOB_TIMEOUT_SECONDS', 300)
        logger_api.debug(f"Waiting for query job {query_job.job_id} to complete (timeout: {timeout_seconds}s)...")
        query_job.result(timeout=timeout_seconds)
        logger_api.debug(f"Fetching DataFrame for completed job {query_job.job_id}...")
        df = query_job.to_dataframe()
        logger_api.info(f"Fetched {len(df)} rows into DataFrame.")
        return df
    except TimeoutError:
        job_id_str = getattr(query_job, 'job_id', 'unknown')
        logger_api.error(f"Query job {job_id_str} timed out after {timeout_seconds} seconds.")
        return pd.DataFrame([{"error": f"Query timed out after {timeout_seconds}s", "query": query}])
    except NotFound as e:
        logger_api.warning(f"Error fetching BigQuery data for export (NotFound): {e}", exc_info=False)
        error_message = f"Table/Resource not found: {e.message}"
        return pd.DataFrame([{"error": error_message, "query": query}])
    except (GoogleAPICallError, Forbidden, ValueError, TypeError) as e:
        logger_api.error(f"Error fetching BigQuery data for export: {e}", exc_info=True)
        error_message = f"Failed to fetch data: {type(e).__name__} - {str(e)}"
        return pd.DataFrame([{"error": error_message, "query": query}])
    except Exception as e:
        logger_api.error(f"Unexpected error fetching BQ data for export: {e}", exc_info=True)
        return pd.DataFrame([{"error": f"Unexpected error: {str(e)}", "query": query}])

# --- NEW HELPER FUNCTION for Datetime Conversion ---
def make_datetime_naive(df: pd.DataFrame) -> pd.DataFrame:
    """Converts timezone-aware datetime columns in a DataFrame to timezone-naive."""
    if df is None or df.empty:
        return df

    for col in df.select_dtypes(include=['datetime64[ns, UTC]', 'datetimetz']).columns:
        try:
            # Check if column actually has timezone info (safety check)
            if getattr(df[col].dt, 'tz', None) is not None:
                logger_api.debug(f"Converting column '{col}' to timezone-naive for Excel export.")
                # Convert to UTC then remove timezone info
                # Using tz_convert(None) is generally safer than tz_localize(None) here
                df[col] = df[col].dt.tz_convert(None)
                # Alternatively, convert to string if preserving datetime object fails:
                # df[col] = df[col].astype(str)
        except Exception as e:
            logger_api.warning(f"Could not convert column '{col}' to naive datetime: {e}. Skipping conversion for this column.")
    return df
# --- END NEW HELPER FUNCTION ---

# --- API Endpoint ---
@export_router.post("/query-to-excel",
                    response_class=Response,
                    responses={
                        200: {"content": {"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {}}},
                        404: {"description": "Job ID not found or results unavailable."},
                        500: {"description": "Internal server error during export generation."},
                        503: {"description": "BigQuery client not available."},
                    })
async def export_query_to_excel(req: QueryExportRequest):
    """
    Exports the result of a BigQuery job, the original SQL, and previews
    of source tables to an Excel file.
    """
    if not api_bigquery_client:
         logger_api.error("Export Endpoint: BigQuery client is None. Check main.py initialization.")
         raise HTTPException(status_code=503, detail="BigQuery client not available.")

    logger_api.info(f"Received Excel export request for Job ID: {req.job_id}, SQL: {req.sql[:50]}...")

    try:
        # 1. Fetch Job Results
        logger_api.info(f"Fetching results for job {req.job_id}...")
        job = api_bigquery_client.get_job(req.job_id, location=req.location)
        if job.state != 'DONE': raise HTTPException(status_code=400, detail=f"Job {req.job_id} not complete (State: {job.state}).")
        if job.error_result: raise HTTPException(status_code=400, detail=f"Job {req.job_id} failed: {job.error_result.get('message')}")

        results_df: pd.DataFrame
        if not job.destination:
            results_df = pd.DataFrame([{"message": "Query did not produce a standard result table (e.g., DML or script)."}])
        else:
            rows_iterator = api_bigquery_client.list_rows(job.destination)
            results_df = rows_iterator.to_dataframe()
        logger_api.info(f"Fetched {len(results_df)} result rows for job {req.job_id}.")

        # 2. Extract Source Tables
        source_tables = extract_fully_qualified_tables(req.sql)

        # 3. Fetch Source Table Previews
        source_data_frames: Dict[str, pd.DataFrame] = {}
        processed_sheet_names: Set[str] = set()

        for table_fqn in source_tables:
            logger_api.info(f"Fetching preview for source table: {table_fqn} (Limit: {MAX_SOURCE_TABLE_ROWS})")
            if 'information_schema' in table_fqn.lower():
                 logger_api.warning(f"Skipping fetch for potential schema table: {table_fqn}")
                 continue
            query = f"SELECT * FROM `{table_fqn}` LIMIT {MAX_SOURCE_TABLE_ROWS}"
            try:
                df = await fetch_bq_data_to_dataframe(query, req.location)
                base_table_name = table_fqn.split('.')[-1]
                sanitized_base_name = re.sub(r'[\\/*?:\[\]]', '_', base_table_name)[:30]
                sheet_name = sanitized_base_name
                count = 1
                while sheet_name.lower() in (name.lower() for name in processed_sheet_names):
                    sheet_name = f"{sanitized_base_name[:28]}_{count}"
                    count += 1
                is_error_df = 'error' in df.columns and 'query' in df.columns and len(df) == 1
                final_sheet_name = f"Error_{sheet_name}" if is_error_df else f"Source_{sheet_name}"
                source_data_frames[final_sheet_name] = df
                processed_sheet_names.add(final_sheet_name)
            except Exception as e:
                 logger_api.error(f"Unexpected error during source table loop for {table_fqn}: {e}", exc_info=True)
                 error_sheet_name = f"UnexpectedError_{table_fqn.split('.')[-1][:18]}"
                 source_data_frames[error_sheet_name] = pd.DataFrame([{"error": f"Unexpected Error in loop: {str(e)}"}])
                 processed_sheet_names.add(error_sheet_name)

        # 4. Create Excel File in Memory
        excel_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            # --- Convert results_df datetimes before writing ---
            results_df_naive = make_datetime_naive(results_df.copy()) # Work on a copy
            results_df_naive.to_excel(writer, sheet_name='Results', index=False)
            logger_api.debug("Added 'Results' sheet.")

            # --- Write Query sheet ---
            pd.DataFrame({'SQL Query': [req.sql]}).to_excel(writer, sheet_name='Query', index=False)
            logger_api.debug("Added 'Query' sheet.")

            # --- Convert source df datetimes before writing ---
            for final_sheet_name, df_to_write in source_data_frames.items():
                df_naive = make_datetime_naive(df_to_write.copy()) # Work on a copy
                df_naive.to_excel(writer, sheet_name=final_sheet_name, index=False)
                logger_api.debug(f"Added '{final_sheet_name}' sheet.")

        excel_buffer.seek(0)

        # 5. Prepare Response
        filename = f"query_export_{req.job_id[:8]}.xlsx"
        headers = {'Content-Disposition': f'attachment; filename="{filename}"'}
        media_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        logger_api.info(f"Successfully generated Excel export: {filename}")
        return Response(content=excel_buffer.getvalue(), media_type=media_type, headers=headers)

    # --- Exception Handling ---
    except NotFound: # Specifically for the initial get_job call
        logger_api.warning(f"Job ID {req.job_id} not found during initial job fetch.")
        raise HTTPException(status_code=404, detail=f"Job ID '{req.job_id}' not found in location '{req.location}'.")
    except HTTPException as http_exc:
        raise http_exc # Re-raise known HTTP exceptions
    except ValueError as ve:
         # Catch potential ValueErrors during Excel writing if conversion failed unexpectedly
         logger_api.error(f"ValueError during Excel export preparation for job {req.job_id}: {ve}", exc_info=True)
         raise HTTPException(status_code=500, detail=f"Failed to format data for Excel: {str(ve)}")
    except GoogleAPICallError as e:
        logger_api.error(f"Google API Error during export for job {req.job_id}: {e}", exc_info=True)
        raise HTTPException(status_code=502, detail=f"Error communicating with Google Cloud: {str(e)}")
    except Exception as e:
        logger_api.error(f"Unexpected error during Excel export for job {req.job_id}: {e}", exc_info=True)
        import traceback
        logger_api.error(traceback.format_exc()) # Log full traceback for debugging
        raise HTTPException(status_code=500, detail=f"Failed to generate export file: {str(e)}")