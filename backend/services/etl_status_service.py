# services/etl_status_service.py
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
import uuid
import os 

from google.cloud import firestore # For firestore.DELETE_FIELD
from google.cloud.firestore_v1.client import Client as FirestoreClient
from google.cloud.firestore_v1.transaction import Transaction, transactional

# Assuming get_firestore_client is correctly defined in services.firestore_service
from .firestore_service import get_firestore_client # Use relative import if in the same package

# Logger setup for this module
logger_etl_status = logging.getLogger(__name__ + "_etl_status_service")
# Basic config if not configured by main app (e.g., when running standalone or in tests)
if not logger_etl_status.handlers:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(),
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# --- Pydantic model for worker callback (ensure this matches main.py and etl.py) ---
from pydantic import BaseModel, Field # Ensure pydantic is imported

class WorkerFileCompletionPayload(BaseModel):
    batch_id: str
    file_id: str
    original_file_name: str
    success: bool
    error_message: Optional[str] = None
    # +++ NEW METRICS FROM WORKER +++
    tables_created_count: Optional[int] = None
    total_rows_loaded: Optional[int] = None
    processing_duration_ms_worker: Optional[int] = None


# --- Collection Names ---
ETL_BATCHES_COLLECTION = "etl_batches_status" # Main batch tracking
USAGE_METRICS_COLLECTION = "usage_metrics"     # Top-level for usage
ETL_FILE_LOGS_SUBCOLLECTION = "etl_file_logs"  # Subcollection for ETL file events


def initialize_batch_status_in_firestore(
    user_uid: str,
    files_to_process_details: List[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """
    Initializes a new batch in Firestore for a list of files.
    Also creates an initial usage log entry for each file.
    Returns a dictionary {'batch_id': str, 'file_ids_map': {original_filename: file_id}} or None.
    """
    if not user_uid or not files_to_process_details:
        logger_etl_status.error("initialize_batch_status_in_firestore: Missing user_uid or files_to_process_details.")
        return None

    db_client: Optional[FirestoreClient] = get_firestore_client()
    if not db_client:
        logger_etl_status.error("initialize_batch_status_in_firestore: Firestore client not available.")
        return None

    batch_id = str(uuid.uuid4())
    creation_time_dt = datetime.now(timezone.utc)
    creation_time_iso = creation_time_dt.isoformat()

    batch_doc_ref = db_client.collection(ETL_BATCHES_COLLECTION).document(batch_id)

    files_map_for_main_batch_status = {}
    file_ids_for_caller_api_map = {}

    for file_detail_item in files_to_process_details:
        file_id = str(uuid.uuid4())
        original_filename = file_detail_item.get("original_file_name", "unknown_filename")
        file_ids_for_caller_api_map[original_filename] = file_id

        files_map_for_main_batch_status[file_id] = {
            "originalFileName": original_filename,
            "gcsObjectName": file_detail_item.get("gcs_object_name"),
            "status": "queued_for_trigger",
            "lastUpdated": creation_time_iso,
            "errorMessage": None,
            "is_multi_header": file_detail_item.get("is_multi_header", False),
            "header_depth": file_detail_item.get("header_depth"),
            "apply_ai_smart_cleanup": file_detail_item.get("apply_ai_smart_cleanup", False),
            "text_normalization_mode": file_detail_item.get("text_normalization_mode"),
            "enable_unpivot": file_detail_item.get("enable_unpivot", False),
            "unpivot_id_cols_str": file_detail_item.get("unpivot_id_cols_str"),
            "unpivot_var_name": file_detail_item.get("unpivot_var_name"),
            "unpivot_value_name": file_detail_item.get("unpivot_value_name"),
            "file_size_bytes": file_detail_item.get("file_size_bytes"), # Ensure this comes from API payload
        }

        # --- Create initial ETL File Usage Log ---
        try:
            etl_usage_log_ref = db_client.collection(USAGE_METRICS_COLLECTION).document(user_uid) \
                                     .collection(ETL_FILE_LOGS_SUBCOLLECTION).document(file_id)

            initial_etl_log_data = {
                "userId": user_uid,
                "batchId": batch_id,
                "fileId": file_id,
                "timestampTriggeredApi": creation_time_dt, # Firestore Timestamp
                "originalFileName": original_filename,
                "gcsObjectName": file_detail_item.get("gcs_object_name"),
                "fileSizeBytes": file_detail_item.get("file_size_bytes"),
                "targetDatasetId": file_detail_item.get("target_dataset_id"), # Crucial for context
                # Log settings used for this ETL run
                "isMultiHeader": file_detail_item.get("is_multi_header", False),
                "headerDepth": file_detail_item.get("header_depth"),
                "applyAiSmartCleanup": file_detail_item.get("apply_ai_smart_cleanup", False),
                "textNormalizationMode": file_detail_item.get("text_normalization_mode"),
                "enableUnpivot": file_detail_item.get("enable_unpivot", False),
                "unpivotIdColsStr": file_detail_item.get("unpivot_id_cols_str"),
                "unpivotVarName": file_detail_item.get("unpivot_var_name"),
                "unpivotValueName": file_detail_item.get("unpivot_value_name"),
                "statusLifecycle": "API_TRIGGER_RECEIVED",
            }
            # Clean None values before setting
            initial_etl_log_data_cleaned = {k: v for k, v in initial_etl_log_data.items() if v is not None}
            
            etl_usage_log_ref.set(initial_etl_log_data_cleaned)
            logger_etl_status.info(f"Initial ETL file usage log CREATED for user {user_uid}, file_id: {file_id} in batch {batch_id}")
        except Exception as e_etl_log:
            logger_etl_status.error(f"FAILED to create initial ETL file usage log for user {user_uid}, file_id {file_id}: {e_etl_log}", exc_info=True)
            # Do not let this failure stop the main batch process, but it needs monitoring.

    main_batch_document_data = {
        "userId": user_uid, # Store userId at the batch level for easier retrieval later
        "creationTime": creation_time_dt, # Firestore Timestamp
        "totalFiles": len(files_to_process_details),
        "files": files_map_for_main_batch_status,
        "overallBatchStatus": "queued_for_trigger",
    }

    try:
        batch_doc_ref.set(main_batch_document_data)
        logger_etl_status.info(f"Firestore: Initialized batch {batch_id} for user {user_uid} with {len(files_to_process_details)} files.")
        return {"batch_id": batch_id, "file_ids_map": file_ids_for_caller_api_map}
    except Exception as e:
        logger_etl_status.error(f"Firestore: Failed to initialize main batch document {batch_id}: {e}", exc_info=True)
        return None


def update_file_status_in_firestore(payload: WorkerFileCompletionPayload) -> bool:
    """
    Updates the status of a single file within a batch and its corresponding usage log.
    Also checks if the entire batch is completed to update overallBatchStatus.
    This function is intended to be called when a worker reports completion.
    """
    client: Optional[FirestoreClient] = get_firestore_client()
    if not client:
        logger_etl_status.error("update_file_status_in_firestore: Firestore client not available.")
        return False

    batch_doc_ref = client.collection(ETL_BATCHES_COLLECTION).document(payload.batch_id)
    log_prefix = f"FS_Update(Batch:{payload.batch_id}, File:{payload.file_id})"

    @transactional
    def _update_in_transaction(transaction: Transaction, main_batch_doc_ref: firestore.DocumentReference, file_payload: WorkerFileCompletionPayload):
        snapshot = main_batch_doc_ref.get(transaction=transaction)
        if not snapshot.exists:
            logger_etl_status.error(f"{log_prefix}: Main batch document '{payload.batch_id}' not found in Firestore for update.")
            return False # Transaction will not commit

        batch_data = snapshot.to_dict()
        if not batch_data: # Should not happen if snapshot.exists
             logger_etl_status.error(f"{log_prefix}: Batch data is empty for batch '{payload.batch_id}'.")
             return False

        user_uid_from_batch = batch_data.get("userId") # Retrieve userId from batch document
        if not user_uid_from_batch:
            logger_etl_status.error(f"{log_prefix}: userId not found in batch document '{payload.batch_id}'. Cannot update usage log.")
            # Continue with main batch status update, but usage log update will be skipped for this file.

        files_in_batch = batch_data.get("files", {})
        if file_payload.file_id not in files_in_batch:
            logger_etl_status.error(f"{log_prefix}: File ID '{file_payload.file_id}' not found within files of batch '{payload.batch_id}'.")
            return False # Transaction will not commit

        # --- 1. Update the specific file's status in the main batch document ---
        new_file_status_in_main_batch = "completed_successfully" if file_payload.success else "completed_error"
        
        file_update_payload = {
            f"files.{file_payload.file_id}.status": new_file_status_in_main_batch,
            f"files.{file_payload.file_id}.lastUpdated": datetime.now(timezone.utc).isoformat(),
            f"files.{file_payload.file_id}.errorMessage": file_payload.error_message if not file_payload.success else firestore.DELETE_FIELD,
            # Store worker-reported metrics in the main batch document as well for quick overview
            f"files.{file_payload.file_id}.tablesCreatedCount": file_payload.tables_created_count,
            f"files.{file_payload.file_id}.totalRowsLoaded": file_payload.total_rows_loaded,
            f"files.{file_payload.file_id}.processingDurationMsWorker": file_payload.processing_duration_ms_worker,
        }
        # Clean None values from this specific update to avoid writing nulls if metrics are not provided
        file_update_payload_cleaned = {k: v for k, v in file_update_payload.items() if v is not None or k.endswith("errorMessage")} # Keep errorMessage for DELETE_FIELD

        transaction.update(main_batch_doc_ref, file_update_payload_cleaned)
        logger_etl_status.debug(f"{log_prefix}: Updated file entry in main batch document.")

        # --- 2. Update the corresponding etl_file_logs document in usage_metrics ---
        if user_uid_from_batch: # Only if we successfully got user_uid
            etl_usage_log_ref = client.collection(USAGE_METRICS_COLLECTION).document(user_uid_from_batch) \
                                      .collection(ETL_FILE_LOGS_SUBCOLLECTION).document(file_payload.file_id)
            
            usage_log_update_data = {
                "timestampWorkerCompleted": datetime.now(timezone.utc), # Firestore Timestamp
                "statusFinal": "SUCCESS" if file_payload.success else "FAILED",
                "errorMessageFinal": file_payload.error_message,
                "tablesCreatedCount": file_payload.tables_created_count,
                "totalRowsLoaded": file_payload.total_rows_loaded,
                "processingDurationMsWorker": file_payload.processing_duration_ms_worker,
            }
            usage_log_update_data_cleaned = {k:v for k,v in usage_log_update_data.items() if v is not None}
            
            # Update the usage log. Assuming it was created by initialize_batch_status_in_firestore.
            # If it might not exist, you could use .set(..., merge=True) or check existence first.
            # For atomicity, it's good to do this within the same transaction.
            transaction.update(etl_usage_log_ref, usage_log_update_data_cleaned)
            logger_etl_status.info(f"{log_prefix}: ETL file usage log updated for user {user_uid_from_batch}, file {file_payload.file_id}")
        
        # --- 3. Check for overall batch completion ---
        # To check overall status, we need the state of *all* files after this current update.
        # So, make a copy of the files data, apply the current update, then check.
        updated_files_data = files_in_batch.copy() # Start with existing files data
        if file_payload.file_id in updated_files_data: # Update the current file's status in our temporary copy
            updated_files_data[file_payload.file_id]["status"] = new_file_status_in_main_batch
            if not file_payload.success:
                 updated_files_data[file_payload.file_id]["errorMessage"] = file_payload.error_message
            else:
                 if "errorMessage" in updated_files_data[file_payload.file_id]:
                     del updated_files_data[file_payload.file_id]["errorMessage"]


        all_files_in_batch_completed = True
        batch_contains_errors = False
        for f_id_iter, f_info_iter in updated_files_data.items(): # Iterate over the temp copy
            if f_info_iter.get("status") not in ["completed_successfully", "completed_error"]:
                all_files_in_batch_completed = False
                break
            if f_info_iter.get("status") == "completed_error":
                batch_contains_errors = True
        
        if all_files_in_batch_completed:
            final_overall_batch_status = "completed_with_errors" if batch_contains_errors else "completed"
            transaction.update(main_batch_doc_ref, {"overallBatchStatus": final_overall_batch_status})
            logger_etl_status.info(f"{log_prefix}: Batch fully processed. Overall status set to: {final_overall_batch_status}")
            # If overall batch completed, also update this status in the individual etl_file_logs for this user/batch?
            # This could be done in a separate step or cloud function for batch completion. For now, focusing on file log.

        return True # Transaction unit succeeded

    try:
        # Execute the transaction
        # The @transactional decorator handles retries automatically.
        transaction_result = _update_in_transaction(client.transaction(), batch_doc_ref, payload)

        if transaction_result:
            logger_etl_status.info(f"{log_prefix}: Firestore status and usage log update transaction completed successfully.")
            return True
        else:
            logger_etl_status.error(f"{log_prefix}: Firestore status and usage log update transaction logic indicated failure (e.g., doc not found).")
            return False
    except Exception as e:
        logger_etl_status.error(f"{log_prefix}: Exception during Firestore transaction for file status update: {e}", exc_info=True)
        return False