import os
import logging
import base64
import json
import tempfile
import atexit
from typing import Optional, Tuple, List

from google.cloud import bigquery, storage, pubsub_v1
from google.oauth2 import service_account
import google.generativeai as genai
from google.generativeai.types import GenerationConfig

# Import config VARIABLES
from config import (
    API_GCP_PROJECT,
    API_GCS_BUCKET,
    API_PUBSUB_TOPIC,
    API_CREDENTIALS_PATH,
    DEFAULT_BQ_LOCATION,
    GEMINI_REQUEST_TIMEOUT,
)

# Load multi-key environment vars
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_API_KEY2 = os.getenv("GEMINI_API_KEY2")
GEMINI_API_KEY3 = os.getenv("GEMINI_API_KEY3")

logger_clients = logging.getLogger(__name__)

# --- Module-level variables for GCP clients ---
_api_bigquery_client: Optional[bigquery.Client] = None
_api_storage_client: Optional[storage.Client] = None
_api_publisher: Optional[pubsub_v1.PublisherClient] = None
_api_topic_path: Optional[str] = None
_temp_cred_file_path_clients: Optional[str] = None
_clients_initialized_flag = False

# Cleanup for temp credentials

def _cleanup_temp_cred_file_clients():
    global _temp_cred_file_path_clients
    if _temp_cred_file_path_clients and os.path.exists(_temp_cred_file_path_clients):
        try:
            os.remove(_temp_cred_file_path_clients)
            logger_clients.info(f"Cleaned up temp credentials file: {_temp_cred_file_path_clients}")
            _temp_cred_file_path_clients = None
        except Exception as e:
            logger_clients.warning(f"Failed to remove temp file: {e}")

# Credentials setup

def setup_credentials_and_environment() -> Optional[str]:
    global _temp_cred_file_path_clients
    if _temp_cred_file_path_clients and os.path.exists(_temp_cred_file_path_clients):
        return _temp_cred_file_path_clients

    encoded = API_CREDENTIALS_PATH
    if not encoded:
        logger_clients.info("No Base64 GCP credentials; using ADC if available.")
        return None

    try:
        info = json.loads(base64.b64decode(encoded).decode())
    except Exception as e:
        logger_clients.error(f"Decode GCP credentials failed: {e}")
        return None

    try:
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as tf:
            json.dump(info, tf)
            path = tf.name
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path
        atexit.register(_cleanup_temp_cred_file_clients)
        _temp_cred_file_path_clients = path
        logger_clients.info(f"Written temp cred file: {path}")
        return path
    except Exception as e:
        logger_clients.error(f"Write temp GCP cred failed: {e}")
        return None

# Initialize GCP clients

def initialize_google_clients() -> Tuple[
    Optional[bigquery.Client],
    Optional[storage.Client],
    Optional[pubsub_v1.PublisherClient],
    Optional[str]
]:
    global _api_bigquery_client, _api_storage_client, _api_publisher, _api_topic_path, _clients_initialized_flag

    if _clients_initialized_flag:
        return _api_bigquery_client, _api_storage_client, _api_publisher, _api_topic_path

    setup_credentials_and_environment()
    project = API_GCP_PROJECT
    if not project:
        logger_clients.critical("GCP_PROJECT not set.")
        _clients_initialized_flag = True
        return None, None, None, None

    try:
        _api_bigquery_client = bigquery.Client(project=project)
    except Exception as e:
        logger_clients.error(f"BigQuery init failed: {e}")

    if API_GCS_BUCKET:
        try:
            _api_storage_client = storage.Client(project=project)
        except Exception as e:
            logger_clients.error(f"Storage init failed: {e}")

    if API_PUBSUB_TOPIC:
        try:
            _api_publisher = pubsub_v1.PublisherClient()
            _api_topic_path = _api_publisher.topic_path(project, API_PUBSUB_TOPIC)
        except Exception as e:
            logger_clients.error(f"Pub/Sub init failed: {e}")

    _clients_initialized_flag = True
    return _api_bigquery_client, _api_storage_client, _api_publisher, _api_topic_path

# --- Multi-key Gemini support (high-level) ---
_GEMINI_KEYS: List[Optional[str]] = [
    GEMINI_API_KEY,
    GEMINI_API_KEY2,
    GEMINI_API_KEY3,
]


def generate_with_key(idx: int, prompt: str, timeout: Optional[int] = GEMINI_REQUEST_TIMEOUT) -> str:
    """
    Generate text with gemini-1.5-flash-latest using API key index 0..2.
    """
    if idx < 0 or idx >= len(_GEMINI_KEYS):
        raise IndexError(f"Gemini key index {idx} out of range")
    key = _GEMINI_KEYS[idx]
    if not key:
        raise ValueError(f"Gemini API key at index {idx} not configured")

    genai.configure(api_key=key)
    model = genai.GenerativeModel(
        model_name="gemini-1.5-flash-latest",
        generation_config=GenerationConfig(),
    )
    resp = model.generate_content(
        prompt,
        request_options={"timeout": timeout}
    )
    return resp.text


def initialize_gemini():
    """Configure high-level Gemini with first available key."""
    for key in _GEMINI_KEYS:
        if key:
            genai.configure(api_key=key)
            logger_clients.info("High-level Gemini configured.")
            return
    logger_clients.warning("No Gemini API keys found; skipping configure.")

# --- End of clients.py ---
