# backend/config.py

import os
import logging
from typing import Optional
from dotenv import load_dotenv
# Removed unused GCP client imports
import google.generativeai as genai

# --- Load Environment Variables ---
load_dotenv()

# --- Logging Setup ---
# Basic config is fine here if this module is imported first
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=log_level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger_config = logging.getLogger(__name__)
logger_config.setLevel(log_level)
logger_config.info("Loading configuration variables...")

# --- Configuration Variables (Centralized) ---
API_GCP_PROJECT = os.getenv("GCP_PROJECT")
API_GCS_BUCKET = os.getenv("GCS_BUCKET")
API_PUBSUB_TOPIC = os.getenv("PUBSUB_TOPIC")
# Reads the env var which contains Base64 content (set by k8s secret)
API_CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
DEFAULT_JOB_TIMEOUT_SECONDS = int(os.getenv("BQ_JOB_TIMEOUT_SECONDS", 300))
DEFAULT_BQ_LOCATION = os.getenv("BQ_LOCATION", "US")
SIGNED_URL_EXPIRATION_MINUTES = int(os.getenv("SIGNED_URL_EXPIRATION_MINUTES", 30))
GEMINI_REQUEST_TIMEOUT = int(os.getenv("GEMINI_TIMEOUT_SECONDS", 120))
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
# Reads the env var containing Base64 Firebase key (set by k8s secret)
FIREBASE_ADMIN_SDK_KEY_PATH = os.getenv("FIREBASE_ADMIN_SDK_KEY_BASE64") # Assuming auth.py uses this exact name
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")

# --- Input Validation ---
if not API_GCP_PROJECT: logger_config.critical("Config Error: Missing GCP_PROJECT")
if not API_GCS_BUCKET: logger_config.warning("Config Warning: GCS_BUCKET not set.")
if not API_PUBSUB_TOPIC: logger_config.warning("Config Warning: PUBSUB_TOPIC not set.")
# Adjusted validation: Check if the variable is set, not if the path exists
if not API_CREDENTIALS_PATH: logger_config.warning("Config Warning: GOOGLE_APPLICATION_CREDENTIALS env var not set. ADC will be attempted by clients.")
if not GEMINI_API_KEY: logger_config.warning("Config Warning: GEMINI_API_KEY not set.")
if not FIREBASE_ADMIN_SDK_KEY_PATH: logger_config.warning("Config Warning: FIREBASE_ADMIN_SDK_KEY_PATH env var not set. Firebase auth may fail.")

# --- Initialize Non-GCP Clients (Example: Gemini) ---
# Keep Gemini init here as it might be needed early or is self-contained
if GEMINI_API_KEY:
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        logger_config.info("Gemini API configured successfully in config.py.")
    except Exception as e_gemini_config:
        logger_config.error(f"Failed to configure Gemini API in config.py: {e_gemini_config}")
else:
    logger_config.info("Gemini client configuration skipped in config.py (GEMINI_API_KEY not set).")


# --- GCP Client Initialization Removed ---
# (The block initializing api_bigquery_client etc. is deleted from here)

logger_config.info("Configuration variable loading complete.")