import uuid
from datetime import datetime, timezone # timedelta might be needed for TTL logic later
from typing import List, Dict, Any, Optional
# services/etl_status_service.py
# …
from .firestore_service import db, get_firestore_client
from google.cloud import firestore_v1 as firestore # For firestore.DELETE_FIELD, explicitly use v1
                                                 # or just from google.cloud import firestore
                                                 # if that's how you import it for db
import logging

# It's better to get the logger instance that FastAPI/Uvicorn is using if possible,
# or ensure this logger is configured by your main app's logging setup.
# If main.py uses logger_api = logging.getLogger("uvicorn.error" ...)
# then here you might want to use the same name, or a child logger.
# For simplicity, let's use a logger specific to this module.
logger_etl_status = logging.getLogger(__name__) # Standard Python logging

# --- Pydantic model for worker callback ---
# Ideally, this lives in a shared schemas.py or models.py
# and is imported here AND in main.py AND in etl.py (worker).
# For now, keeping it here as per your structure.
from pydantic import BaseModel, Field
from google.cloud.firestore_v1 import transactional
class WorkerFileCompletionPayload(BaseModel): # Renamed for clarity (was also in main.py)
    batch_id: str
    file_id: str
    original_file_name: str # Changed from file_name to match what initialize_batch_status creates
    success: bool
    error_message: Optional[str] = None


# --- Collection Name ---
ETL_BATCHES_COLLECTION = "etlBatches"

def initialize_batch_status_in_firestore(
    user_uid: str, 
    files_to_process: List[Dict[str, Any]] # Expects list of dicts
) -> Optional[Dict[str, Any]]: # Return dict with batch_id and file_ids_map
    """
    Initializes a new batch in Firestore for a list of files.
    Returns a dictionary {'batch_id': str, 'file_ids_map': {original_filename: file_id}} or None.
    """
    if not user_uid or not files_to_process:
        logger_etl_status.error("initialize_batch_status: Missing user_uid or files_to_process.")
        return None

    batch_id = str(uuid.uuid4())
    client = get_firestore_client()
    batch_doc_ref = client.collection(ETL_BATCHES_COLLECTION).document(batch_id)

    files_map_for_firestore = {}
    file_ids_for_caller_map = {} # To return {original_filename: file_id}

    for file_detail in files_to_process:
        file_id = str(uuid.uuid4())
        original_fn = file_detail["original_file_name"]
        file_ids_for_caller_map[original_fn] = file_id # Map original name to new file_id

        files_map_for_firestore[file_id] = {
            "originalFileName": original_fn,
            "gcsObjectName": file_detail["gcs_object_name"],
            "status": "queued_for_trigger",
            "lastUpdated": datetime.now(timezone.utc).isoformat(),
            "errorMessage": None,
            "is_multi_header": file_detail.get("is_multi_header", False),
            "header_depth": file_detail.get("header_depth") ,
            "apply_ai_smart_cleanup": file_detail.get("apply_ai_smart_cleanup", False),
            "text_normalization_mode": file_detail.get("text_normalization_mode"),
            "enable_unpivot": file_detail.get("enable_unpivot", False),
            "unpivot_id_cols_str": file_detail.get("unpivot_id_cols_str"), # Log the string version user provided
            "unpivot_var_name": file_detail.get("unpivot_var_name"),
            "unpivot_value_name": file_detail.get("unpivot_value_name"),
        }

    initial_batch_data = {
        "userId": user_uid,
        "creationTime": datetime.now(timezone.utc).isoformat(),
        "totalFiles": len(files_to_process),
        "files": files_map_for_firestore, # This is the map of file_id -> details
        "overallBatchStatus": "processing"
    }

    try:
        batch_doc_ref.set(initial_batch_data)
        logger_etl_status.info(f"Firestore: Initialized batch {batch_id} for user {user_uid} with {len(files_to_process)} files.")
        return {"batch_id": batch_id, "file_ids_map": file_ids_for_caller_map}
    except Exception as e:
        logger_etl_status.error(f"Firestore: Failed to initialize batch {batch_id}: {e}", exc_info=True)
        return None



def update_file_status_in_firestore(payload: WorkerFileCompletionPayload) -> bool:
    client = get_firestore_client()
    if not client: # Add a check for client availability
        logger_etl_status.error("Firestore client not available in update_file_status_in_firestore.")
        return False

    batch_doc_ref = client.collection(ETL_BATCHES_COLLECTION).document(payload.batch_id)
    log_prefix = f"FS_Update (Batch: {payload.batch_id}, File: {payload.file_id})"

    # Define the transactional unit of work as a regular function
    # The @transactional decorator needs a function that accepts the transaction object as its first argument
    # and then any other arguments you want to pass to it.
    @transactional # Use the SYNCHRONOUS transactional decorator
    def _update_in_transaction_sync(transaction_obj, doc_ref_to_update, file_payload_data):
        # All operations use transaction_obj and are synchronous
        snapshot = doc_ref_to_update.get(transaction=transaction_obj) 
        if not snapshot.exists:
            logger_etl_status.error(f"{log_prefix}: Batch document not found in Firestore.")
            return False # Indicate failure within transaction logic

        batch_data = snapshot.to_dict()
        if not batch_data or "files" not in batch_data or file_payload_data.file_id not in batch_data["files"]:
            logger_etl_status.error(f"{log_prefix}: File ID {file_payload_data.file_id} not found within batch files.")
            return False

        file_status_path = f"files.{file_payload_data.file_id}.status"
        file_error_path = f"files.{file_payload_data.file_id}.errorMessage"
        file_updated_path = f"files.{file_payload_data.file_id}.lastUpdated"
        new_file_status = "completed_success" if file_payload_data.success else "completed_error"

        update_payload_for_firestore = {
            file_status_path: new_file_status,
            file_updated_path: datetime.now(timezone.utc).isoformat()
        }
        if not file_payload_data.success:
            update_payload_for_firestore[file_error_path] = file_payload_data.error_message
        else:
            update_payload_for_firestore[file_error_path] = firestore.DELETE_FIELD

        temp_files_data = batch_data.get("files", {})
        if file_payload_data.file_id in temp_files_data:
             temp_files_data[file_payload_data.file_id]["status"] = new_file_status

        all_files_processed = True
        batch_has_errors = False
        for f_id, f_info in temp_files_data.items():
            if f_info.get("status") not in ["completed_success", "completed_error"]:
                all_files_processed = False
                break
            if f_info.get("status") == "completed_error":
                batch_has_errors = True

        if all_files_processed:
            overall_status_update = "completed_with_errors" if batch_has_errors else "completed"
            update_payload_for_firestore["overallBatchStatus"] = overall_status_update
            logger_etl_status.info(f"{log_prefix}: Batch completion detected. New overall status: {overall_status_update}")

        transaction_obj.update(doc_ref_to_update, update_payload_for_firestore) # Use transaction_obj
        return True # Indicate success of this transactional unit

    try:
        transaction = client.transaction() # Get a transaction object from the sync client
        # Run the decorated function. The decorator handles retries.
        # Pass additional args after the transaction object placeholder
        update_successful = _update_in_transaction_sync(transaction, batch_doc_ref, payload)

        if update_successful: # This will now be a boolean from your decorated function
            logger_etl_status.info(f"{log_prefix}: Firestore status updated successfully.")
        else:
            # This 'else' will be hit if your _update_in_transaction_sync explicitly returned False
            logger_etl_status.error(f"{log_prefix}: Firestore status update transaction logic indicated failure.")
        return update_successful

    except Exception as e:
        logger_etl_status.error(f"{log_prefix}: Exception during Firestore transaction: {e}", exc_info=True)
        return False