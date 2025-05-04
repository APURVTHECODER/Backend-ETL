# backend/clients.py

import os
import logging
import base64
import json
import tempfile
import atexit
from typing import Optional, Tuple

from google.cloud import bigquery, storage, pubsub_v1
from google.oauth2 import service_account
import google.generativeai as genai

# Import config VARIABLES
from config import (
    API_GCP_PROJECT, API_GCS_BUCKET, API_PUBSUB_TOPIC, API_CREDENTIALS_PATH,
    DEFAULT_BQ_LOCATION, GEMINI_API_KEY, GEMINI_REQUEST_TIMEOUT
)

logger_clients = logging.getLogger(__name__) # Using module name for logger

# --- Module-level variables to HOLD the initialized client instances ---
_api_bigquery_client: Optional[bigquery.Client] = None
_api_storage_client: Optional[storage.Client] = None
_api_publisher: Optional[pubsub_v1.PublisherClient] = None
_api_topic_path: Optional[str] = None
_temp_cred_file_path_clients: Optional[str] = None # Renamed for clarity
_clients_initialized_flag = False # Flag to prevent re-initialization

def _cleanup_temp_cred_file_clients():
    global _temp_cred_file_path_clients
    if _temp_cred_file_path_clients and os.path.exists(_temp_cred_file_path_clients):
        try:
            os.remove(_temp_cred_file_path_clients)
            logger_clients.info(f"Cleaned up temp credentials file: {_temp_cred_file_path_clients}")
            _temp_cred_file_path_clients = None
        except Exception as e:
            logger_clients.warning(f"Could not remove temp credentials file {_temp_cred_file_path_clients}: {e}")

def setup_credentials_and_environment() -> Optional[str]:
    """
    Handles decoding/writing credentials from API_CREDENTIALS_PATH (Base64 content)
    and setting the GOOGLE_APPLICATION_CREDENTIALS environment variable to a temp file.
    Returns the path to the temp file if created, or None.
    """
    global _temp_cred_file_path_clients
    if _temp_cred_file_path_clients and os.path.exists(_temp_cred_file_path_clients):
        logger_clients.debug("Credentials temp file already exists, GOOGLE_APPLICATION_CREDENTIALS likely set.")
        return _temp_cred_file_path_clients

    encoded_key_content = API_CREDENTIALS_PATH # From config.py

    if encoded_key_content:
        logger_clients.info("Attempting to decode/write Base64 key from config (API_CREDENTIALS_PATH)...")
        try:
            decoded_key_bytes = base64.b64decode(encoded_key_content)
            decoded_key_str = decoded_key_bytes.decode('utf-8')
            key_info = json.loads(decoded_key_str)
            logger_clients.info("Successfully decoded/parsed Base64 key.")

            try:
                with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
                    json.dump(key_info, temp_file)
                    temp_file_path = temp_file.name
                logger_clients.info(f"Decoded credentials written to temporary file: {temp_file_path}")
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = temp_file_path
                logger_clients.info(f"Set GOOGLE_APPLICATION_CREDENTIALS env var to: {temp_file_path}")
                _temp_cred_file_path_clients = temp_file_path
                atexit.register(_cleanup_temp_cred_file_clients) # Register cleanup here
                return temp_file_path

            except Exception as temp_file_error:
                logger_clients.error(f"Failed to write decoded credentials to temp file: {temp_file_error}", exc_info=True)
                _temp_cred_file_path_clients = None
                os.environ.pop('GOOGLE_APPLICATION_CREDENTIALS', None)
                return None

        except (base64.binascii.Error, json.JSONDecodeError, UnicodeDecodeError, ValueError, TypeError) as decode_error:
            logger_clients.error(f"Failed to decode/parse Base64 key: {decode_error}. Check env var content.")
            _temp_cred_file_path_clients = None
            os.environ.pop('GOOGLE_APPLICATION_CREDENTIALS', None)
            return None
    else:
         logger_clients.info("API_CREDENTIALS_PATH (Base64 content) not set. Will rely on ADC if available.")
         return None

def initialize_google_clients() -> Tuple[Optional[bigquery.Client], Optional[storage.Client], Optional[pubsub_v1.PublisherClient], Optional[str]]:
    """
    Initializes Google Cloud clients and STORES them in module-level variables.
    Returns the tuple of initialized clients (or None for failed ones).
    SHOULD BE CALLED ONLY ONCE AT APP STARTUP.
    """
    global _api_bigquery_client, _api_storage_client, _api_publisher, _api_topic_path
    global _clients_initialized_flag

    if _clients_initialized_flag:
         logger_clients.warning("initialize_google_clients called again. Returning stored client states.")
         # Return the already initialized (or failed) clients
         return _api_bigquery_client, _api_storage_client, _api_publisher, _api_topic_path

    logger_clients.info("Attempting to initialize Google Client instances...")

    # Step 1: Ensure credentials environment is set up
    temp_file_path = setup_credentials_and_environment() # Returns path if temp file created
    auth_method = "temp key file" if temp_file_path else "ADC"
    logger_clients.info(f"Client initialization will proceed using: {auth_method}")

    gcp_project_id = API_GCP_PROJECT

    if not gcp_project_id:
        logger_clients.error("Cannot initialize clients: GCP_PROJECT not set.")
        _clients_initialized_flag = True # Mark as attempted
        return None, None, None, None

    # --- Initialize Clients and store them in module-level variables ---
    # BigQuery
    try:
        logger_clients.debug("Initializing BigQuery client...")
        _api_bigquery_client = bigquery.Client(project=gcp_project_id)
        logger_clients.info(f"BigQuery Client Initialized. Project: {gcp_project_id}")
    except Exception as e_bq:
        logger_clients.error(f"Failed to initialize BigQuery Client: {e_bq}", exc_info=True)
        _api_bigquery_client = None # Ensure it's None on failure

    # Storage
    if API_GCS_BUCKET:
        try:
            logger_clients.debug("Initializing Storage client...")
            _api_storage_client = storage.Client(project=gcp_project_id)
            logger_clients.info(f"Storage Client Initialized for bucket: {API_GCS_BUCKET}")
        except Exception as e_storage:
            logger_clients.error(f"Failed to initialize Storage Client: {e_storage}", exc_info=True)
            _api_storage_client = None
    else:
        logger_clients.info("Storage client skipped (GCS_BUCKET not set).")


    # Pub/Sub
    if API_PUBSUB_TOPIC:
        try:
            logger_clients.debug("Initializing Pub/Sub client...")
            _api_publisher = pubsub_v1.PublisherClient()
            logger_clients.info(f"Pub/Sub Publisher Client Initialized.")
            if _api_publisher:
                _api_topic_path = _api_publisher.topic_path(gcp_project_id, API_PUBSUB_TOPIC)
                logger_clients.info(f"Pub/Sub topic path set: {_api_topic_path}")
        except Exception as e_pubsub:
            logger_clients.error(f"Failed to initialize Pub/Sub Client: {e_pubsub}", exc_info=True)
            _api_publisher = None
            _api_topic_path = None
    else:
        logger_clients.info("Pub/Sub client skipped (PUBSUB_TOPIC not set).")

    _clients_initialized_flag = True # Mark initialization attempt as complete
    logger_clients.info(f"Google Cloud client initialization routine finished.")
    # Return the stored clients
    return _api_bigquery_client, _api_storage_client, _api_publisher, _api_topic_path


def initialize_gemini():
    # ... (Gemini initialization unchanged) ...
    """Initializes the Gemini client if API key is provided."""
    # Check if already configured (simple check, might need refinement)
    if getattr(genai, '_client', None):
        logger_clients.debug("Gemini API likely already configured.")
        return

    if GEMINI_API_KEY:
        try:
            genai.configure(api_key=GEMINI_API_KEY)
            logger_clients.info("Gemini API configured successfully.")
        except Exception as e_gemini_config:
            logger_clients.error(f"Failed to configure Gemini API: {e_gemini_config}")
    else:
        logger_clients.info("Gemini client configuration skipped (GEMINI_API_KEY not set).")