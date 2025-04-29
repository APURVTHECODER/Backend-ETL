# config.py

import os
import logging
from typing import Optional
from dotenv import load_dotenv
from google.cloud import bigquery, storage, pubsub_v1
from google.oauth2 import service_account
import google.generativeai as genai

# --- Load Environment Variables ---
load_dotenv()

# --- Logging Setup (Centralized) ---
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=log_level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# Use a generic logger name or pass the specific uvicorn logger if needed later
logger_config = logging.getLogger(__name__)
logger_config.setLevel(log_level)
logger_config.info("Configuration and clients initializing...")

# --- Configuration Variables (Centralized) ---
API_GCP_PROJECT = os.getenv("GCP_PROJECT")
API_GCS_BUCKET = os.getenv("GCS_BUCKET")
API_PUBSUB_TOPIC = os.getenv("PUBSUB_TOPIC")
API_CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
DEFAULT_JOB_TIMEOUT_SECONDS = int(os.getenv("BQ_JOB_TIMEOUT_SECONDS", 300))
DEFAULT_BQ_LOCATION = os.getenv("BQ_LOCATION", "US")
SIGNED_URL_EXPIRATION_MINUTES = int(os.getenv("SIGNED_URL_EXPIRATION_MINUTES", 30))
GEMINI_REQUEST_TIMEOUT = int(os.getenv("GEMINI_TIMEOUT_SECONDS", 120))
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
FIREBASE_ADMIN_SDK_KEY_PATH = os.getenv("FIREBASE_ADMIN_SDK_KEY_PATH")
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")

# --- Input Validation ---
if not API_GCP_PROJECT: logger_config.critical("Config Error: Missing required environment variable GCP_PROJECT")
if not API_GCS_BUCKET: logger_config.warning("Config Warning: GCS_BUCKET not set. Upload URL endpoint might fail.")
if not API_PUBSUB_TOPIC: logger_config.warning("Config Warning: PUBSUB_TOPIC not set. ETL trigger endpoint might fail.")
if API_CREDENTIALS_PATH and not os.path.exists(API_CREDENTIALS_PATH): logger_config.warning(f"Config Warning: Credentials file not found at: {API_CREDENTIALS_PATH}. Attempting ADC.")
if not GEMINI_API_KEY: logger_config.warning("Config Warning: GEMINI_API_KEY not set. AI features might fail.")

# --- Initialize API Clients (Centralized) ---
api_bigquery_client: Optional[bigquery.Client] = None
api_storage_client: Optional[storage.Client] = None
api_publisher: Optional[pubsub_v1.PublisherClient] = None
api_topic_path: Optional[str] = None

# Initialize Gemini if key exists
if GEMINI_API_KEY:
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        logger_config.info("Gemini API configured successfully.")
    except Exception as e_gemini_config:
        logger_config.error(f"Failed to configure Gemini API: {e_gemini_config}")
        GEMINI_API_KEY = None # Ensure it's None if config failed
else:
    logger_config.info("Gemini client configuration skipped (GEMINI_API_KEY not set).")

# Initialize GCP Clients
try:
    gcp_project_id = API_GCP_PROJECT
    creds = None
    using_adc = False

    if API_CREDENTIALS_PATH and os.path.exists(API_CREDENTIALS_PATH):
        creds = service_account.Credentials.from_service_account_file(API_CREDENTIALS_PATH)
        logger_config.info(f"Using credentials from: {API_CREDENTIALS_PATH}")
    elif gcp_project_id:
        using_adc = True
        logger_config.info("Using Application Default Credentials (ADC).")
    else:
        logger_config.error("Cannot initialize clients: GCP_PROJECT not set and no valid credentials path provided.")
        # Optional: raise an exception here if GCP clients are absolutely mandatory
        # raise RuntimeError("GCP Project ID or Credentials are required to initialize clients.")

    if gcp_project_id:
        # BigQuery Client
        try:
            api_bigquery_client = bigquery.Client(credentials=creds, project=gcp_project_id) if not using_adc else bigquery.Client(project=gcp_project_id)
            logger_config.info(f"BigQuery Client Initialized. Project: {gcp_project_id}, Default Location: {DEFAULT_BQ_LOCATION}")
        except Exception as e_bq:
            logger_config.error(f"Failed to initialize BigQuery Client: {e_bq}", exc_info=True)
            api_bigquery_client = None # Ensure it's None on failure

        # Storage Client
        if API_GCS_BUCKET:
            try:
                api_storage_client = storage.Client(credentials=creds, project=gcp_project_id) if not using_adc else storage.Client(project=gcp_project_id)
                logger_config.info(f"Storage Client Initialized for bucket: {API_GCS_BUCKET}")
            except Exception as e_storage:
                logger_config.error(f"Failed to initialize Storage Client: {e_storage}", exc_info=True)
                api_storage_client = None
        else:
            logger_config.info("Storage client initialization skipped (GCS_BUCKET not set).")

        # Pub/Sub Client
        if API_PUBSUB_TOPIC:
            try:
                api_publisher = pubsub_v1.PublisherClient(credentials=creds) if not using_adc else pubsub_v1.PublisherClient()
                api_topic_path = api_publisher.topic_path(gcp_project_id, API_PUBSUB_TOPIC)
                logger_config.info(f"Pub/Sub Publisher Client Initialized for topic: {api_topic_path}")
            except Exception as e_pubsub:
                logger_config.error(f"Failed to initialize Pub/Sub Client: {e_pubsub}", exc_info=True)
                api_publisher = None
                api_topic_path = None
        else:
            logger_config.info("Pub/Sub client initialization skipped (PUBSUB_TOPIC not set).")

except Exception as e:
    logger_config.critical(f"Fatal Error during client initialization setup: {e}", exc_info=True)
    # Ensure all clients are None if master try block fails
    api_bigquery_client = None
    api_storage_client = None
    api_publisher = None
    # Optional: Re-raise the exception to prevent the app from starting incorrectly
    # raise e

logger_config.info("Configuration and clients initialization complete.")