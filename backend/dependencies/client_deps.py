# backend/dependencies/client_deps.py

from typing import Optional, Tuple # Added Tuple
from fastapi import HTTPException, status
from google.cloud import bigquery, storage, pubsub_v1
import logging

# Import the INITIALIZER function directly
from clients import initialize_google_clients

logger_deps = logging.getLogger(__name__)

# --- Store client instances RETRIEVED by this module ---
# We will populate these ONCE during the first request using the initializer
_bq_client: Optional[bigquery.Client] = None
_storage_client: Optional[storage.Client] = None
_publisher: Optional[pubsub_v1.PublisherClient] = None
_topic_path: Optional[str] = None
_init_done_by_deps = False # Flag specific to this module


def _ensure_clients_initialized_dependency_scope():
    """
    Calls the main initializer if not already done by this dependency module,
    and stores the results locally within this module's scope.
    """
    global _bq_client, _storage_client, _publisher, _topic_path, _init_done_by_deps
    if not _init_done_by_deps:
        logger_deps.debug("First request reaching dependency, calling main client initializer...")
        try:
            # Call the function from clients.py and store results LOCALLY
            bq, storage_c, pub, topic = initialize_google_clients()
            _bq_client = bq
            _storage_client = storage_c
            _publisher = pub
            _topic_path = topic
            _init_done_by_deps = True # Mark as done within this module's lifecycle
            logger_deps.info("Dependency: Main client initialization function called.")
        except Exception as e:
             logger_deps.error(f"Dependency: Exception during main client initialization call: {e}", exc_info=True)
             _init_done_by_deps = True # Mark attempted even on error

# --- Dependency Getters ---

def get_bigquery_client() -> bigquery.Client:
    _ensure_clients_initialized_dependency_scope() # Ensure init ran
    if _bq_client is None:
        logger_deps.error("BigQuery client requested but is None after initialization call.")
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="BigQuery Service Unavailable")
    return _bq_client

def get_storage_client() -> storage.Client:
    _ensure_clients_initialized_dependency_scope()
    if _storage_client is None:
        logger_deps.error("Storage client requested but is None after initialization call.")
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Storage Service Unavailable")
    return _storage_client

def get_pubsub_publisher() -> pubsub_v1.PublisherClient:
    _ensure_clients_initialized_dependency_scope()
    if _publisher is None:
        logger_deps.error("PubSub publisher requested but is None after initialization call.")
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Messaging Service Unavailable")
    return _publisher

def get_pubsub_topic_path() -> str:
    _ensure_clients_initialized_dependency_scope()
    if _topic_path is None:
        logger_deps.error("PubSub topic path requested but is None after initialization call.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Pub/Sub topic path not configured or client init failed")
    return _topic_path