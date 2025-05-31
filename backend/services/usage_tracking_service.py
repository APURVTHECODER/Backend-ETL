# services/usage_tracking_service.py
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from google.cloud.firestore_v1.client import Client # For type hinting

from services.firestore_service import get_firestore_client # Your existing helper

logger_usage = logging.getLogger(__name__)

def log_nl_query_event(
    user_id: str,
    dataset_id: str,
    ai_mode: str,
    natural_language_prompt: str,
    generated_sql: Optional[str],
    job_id: Optional[str],
    job_status: str, # "SUBMITTED", "SUCCESS", "FAILED"
    job_error_message: Optional[str] = None,
    rows_processed_by_job: Optional[int] = None,
    bytes_processed_by_job: Optional[int] = None,
    duration_ms_job: Optional[int] = None,
    api_call_duration_ms: Optional[int] = None,
    selected_tables: Optional[List[str]] = None,
    selected_columns: Optional[List[str]] = None,
):
    try:
        db: Client = get_firestore_client()
        doc_ref = db.collection("usage_metrics").document(user_id) \
                    .collection("nl_query_logs").document() # Auto-generate document ID

        log_data = {
            "userId": user_id,
            "timestamp": datetime.now(timezone.utc), # Firestore server timestamp can also be used
            "datasetId": dataset_id,
            "aiMode": ai_mode,
            "naturalLanguagePrompt": natural_language_prompt,
            "generatedSql": generated_sql,
            "jobId": job_id,
            "jobStatus": job_status,
            "jobErrorMessage": job_error_message,
            "rowsProcessedByJob": rows_processed_by_job,
            "bytesProcessedByJob": bytes_processed_by_job,
            "durationMsJob": duration_ms_job,
            "apiCallDurationMs": api_call_duration_ms,
            "selectedTables": selected_tables or [], # Ensure it's a list
            "selectedColumns": selected_columns or [], # Ensure it's a list
        }
        # Remove None values to keep Firestore docs clean
        log_data_cleaned = {k: v for k, v in log_data.items() if v is not None}
        
        doc_ref.set(log_data_cleaned)
        logger_usage.info(f"NL Query event logged for user {user_id}, job {job_id or 'N/A'}")
    except Exception as e:
        logger_usage.error(f"Failed to log NL Query event for user {user_id}: {e}", exc_info=True)

# Add log_etl_file_event later