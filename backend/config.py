import os
import logging
from typing import Optional
from dotenv import load_dotenv
import google.generativeai as genai

# --- Load Environment Variables ---
load_dotenv()

# --- Logging Setup ---
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger_config = logging.getLogger(__name__)
logger_config.setLevel(log_level)
logger_config.info("Loading configuration variables...")

# --- Configuration Variables (Centralized) ---
API_GCP_PROJECT = os.getenv("GCP_PROJECT")
API_GCS_BUCKET = os.getenv("GCS_BUCKET")
API_PUBSUB_TOPIC = os.getenv("PUBSUB_TOPIC")
# Reads Base64 GCP credentials (k8s secret)
API_CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

DEFAULT_JOB_TIMEOUT_SECONDS = int(os.getenv("BQ_JOB_TIMEOUT_SECONDS", 300))
DEFAULT_BQ_LOCATION = os.getenv("BQ_LOCATION", "US")
SIGNED_URL_EXPIRATION_MINUTES = int(os.getenv("SIGNED_URL_EXPIRATION_MINUTES", 30))

GEMINI_REQUEST_TIMEOUT = int(os.getenv("GEMINI_TIMEOUT_SECONDS", 120))

# Multi-key support for Gemini
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_API_KEY2 = os.getenv("GEMINI_API_KEY2")
GEMINI_API_KEY3 = os.getenv("GEMINI_API_KEY3")

# Firebase admin SDK (Base64)
FIREBASE_ADMIN_SDK_KEY_PATH = os.getenv("FIREBASE_ADMIN_SDK_KEY_BASE64")

ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")

# --- Input Validation ---
if not API_GCP_PROJECT:
    logger_config.critical("Config Error: Missing GCP_PROJECT")
if not API_GCS_BUCKET:
    logger_config.warning("Config Warning: GCS_BUCKET not set.")
if not API_PUBSUB_TOPIC:
    logger_config.warning("Config Warning: PUBSUB_TOPIC not set.")

if not API_CREDENTIALS_PATH:
    logger_config.warning(
        "Config Warning: GOOGLE_APPLICATION_CREDENTIALS env var not set. ADC will be attempted."
    )

# Warn per Gemini key
if not GEMINI_API_KEY:
    logger_config.warning("Config Warning: GEMINI_API_KEY not set.")
if not GEMINI_API_KEY2:
    logger_config.warning("Config Warning: GEMINI_API_KEY2 not set.")
if not GEMINI_API_KEY3:
    logger_config.warning("Config Warning: GEMINI_API_KEY3 not set.")

if not FIREBASE_ADMIN_SDK_KEY_PATH:
    logger_config.warning(
        "Config Warning: FIREBASE_ADMIN_SDK_KEY_PATH env var not set. Firebase auth may fail."
    )

# --- Initialize Non-GCP Clients (Gemini) ---
# Only configure high-level genai with the first available key
for key_name, key in [
    ("GEMINI_API_KEY", GEMINI_API_KEY),
    ("GEMINI_API_KEY2", GEMINI_API_KEY2),
    ("GEMINI_API_KEY3", GEMINI_API_KEY3),
]:
    if key:
        try:
            genai.configure(api_key=key)
            logger_config.info(f"Gemini API configured successfully using {key_name}.")
            break
        except Exception as e:
            logger_config.error(
                f"Failed to configure Gemini API with {key_name}: {e}",
                exc_info=True
            )
else:
    logger_config.info(
        "All Gemini API keys missing or failed to configure. No high-level Gemini client initialized."
    )

logger_config.info("Configuration variable loading complete.")
