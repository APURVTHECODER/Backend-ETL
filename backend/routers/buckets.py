# routers/buckets.py

import os
import logging
import re
from typing import List
from google.oauth2 import service_account
from google.cloud import storage
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field, validator
from google.cloud import storage
from google.api_core.exceptions import Conflict, Forbidden, GoogleAPICallError
from auth import verify_token # Assuming your auth verification is here
from typing import Optional

# Reuse logger from main app if possible, or create a specific one
logger_bucket = logging.getLogger(__name__) # Or reference logger_api from main

# Configuration - Could be passed in or read from env again
GCP_PROJECT = os.getenv("GCP_PROJECT")
DEFAULT_BUCKET_LOCATION = os.getenv("GCS_BUCKET_LOCATION", "US") # Default location

# --- Input Validation for Bucket Names (GCS Rules) ---
# https://cloud.google.com/storage/docs/naming-buckets
def validate_bucket_name(name: str) -> str:
    if not 3 <= len(name) <= 63:
        raise ValueError("Bucket name must be between 3 and 63 characters long.")
    if not re.fullmatch(r"[a-z0-9][a-z0-9._-]*[a-z0-9]", name):
         raise ValueError("Bucket names must contain only lowercase letters, numbers, dots (.), hyphens (-), and underscores (_), and must start and end with a number or letter.")
    if name.startswith("goog"):
        raise ValueError("Bucket names cannot start with 'goog'.")
    if '.' in name and len(name) > 222:
        raise ValueError("Bucket names containing dots cannot exceed 222 characters.")
    if re.match(r"(\d{1,3}\.){3}\d{1,3}", name):
        raise ValueError("Bucket names cannot be represented as an IP address.")
    # Add more checks if needed (e.g., reserved names)
    return name

# --- Pydantic Models ---
class CreateBucketRequest(BaseModel):
    bucket_name: str = Field(..., description="Globally unique name for the new bucket.")
    location: str = Field(DEFAULT_BUCKET_LOCATION, description="GCP location for the bucket (e.g., US, EU, ASIA-SOUTHEAST1).")

    # Add validator for bucket name
    @validator('bucket_name')
    def name_must_comply_gcs_rules(cls, v):
        try:
            return validate_bucket_name(v)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

class BucketResponse(BaseModel):
    name: str
    location: Optional[str] = None # Location might not always be available immediately or easily
    # Add other relevant fields like timeCreated if needed

# --- Router Definition ---
router = APIRouter(
    prefix="/api/buckets",
    tags=["Buckets"],
    dependencies=[Depends(verify_token)] # Protect bucket routes
)

# --- API Endpoints ---

@router.post("/create", response_model=BucketResponse)
async def create_gcs_bucket(req: CreateBucketRequest):
    """Creates a new Google Cloud Storage bucket."""
    # Use the storage client initialized in main.py if possible and passed
    # For simplicity here, we re-initialize, ensure consistent project/creds
    try:
        # Ensure consistent client initialization as in main.py
        # Reuse api_storage_client if it's appropriately scoped or re-init
        # Using re-init here for clarity, assuming ADC or creds file works
        KEY_PATH = "sl-etb-bot.json"
        creds = service_account.Credentials.from_service_account_file(KEY_PATH)
        storage_client = storage.Client(project="crafty-tracker-457215-g6", credentials=creds)
    except Exception as e:
         logger_bucket.error(f"Failed to initialize storage client for bucket creation: {e}", exc_info=True)
         raise HTTPException(status_code=503, detail="Storage service client unavailable.")

    logger_bucket.info(f"Attempting to create bucket: {req.bucket_name} in location: {req.location}")

    try:
        # Check if bucket already exists (lookup is cheaper than trying create)
        # Note: This has race conditions, create_bucket handles actual conflict
        existing_bucket = storage_client.lookup_bucket(req.bucket_name)
        if existing_bucket:
            logger_bucket.warning(f"Bucket creation failed: name '{req.bucket_name}' already exists.")
            raise HTTPException(status_code=409, detail=f"Bucket name '{req.bucket_name}' already exists.") # 409 Conflict

        # Define the new bucket object
        bucket = storage_client.bucket(req.bucket_name)
        bucket.storage_class = "STANDARD" # Or other desired class

        # Create the bucket
        new_bucket = storage_client.create_bucket(bucket, location=req.location)
        logger_bucket.info(f"Successfully created bucket: {new_bucket.name} in location: {req.location}")

        return BucketResponse(name=new_bucket.name, location=req.location) # Return request location as create might not return it

    except Conflict: # Should be caught by lookup, but handle race condition
        logger_bucket.warning(f"Bucket creation conflict: name '{req.bucket_name}' likely created concurrently.")
        raise HTTPException(status_code=409, detail=f"Bucket name '{req.bucket_name}' already exists (conflict).")
    except Forbidden as e:
        logger_bucket.error(f"Permission denied creating bucket '{req.bucket_name}': {e}", exc_info=True)
        raise HTTPException(status_code=403, detail="Permission denied to create bucket. Check service account roles (needs storage.admin or similar).")
    except GoogleAPICallError as e:
        logger_bucket.error(f"GCP API error creating bucket '{req.bucket_name}': {e}", exc_info=True)
        raise HTTPException(status_code=502, detail=f"Error communicating with Cloud Storage API: {e.message}")
    except Exception as e:
        logger_bucket.error(f"Unexpected error creating bucket '{req.bucket_name}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred during bucket creation.")


@router.get("", response_model=List[BucketResponse])
async def list_gcs_buckets():
    """Lists accessible buckets within the project."""
    try:
        # Re-initialize client for safety, or reuse api_storage_client
        storage_client = storage.Client(project=GCP_PROJECT)
    except Exception as e:
         logger_bucket.error(f"Failed to initialize storage client for listing buckets: {e}", exc_info=True)
         raise HTTPException(status_code=503, detail="Storage service client unavailable.")

    logger_bucket.info(f"Listing buckets for project: {GCP_PROJECT}")
    try:
        buckets_iterator = storage_client.list_buckets()
        # Note: Accessing bucket.location can sometimes require extra permissions or be slow.
        # It might be better to just return names if location isn't strictly needed for selection.
        # We'll try to include it but be aware it might fail or be None.
        results = [
            BucketResponse(name=b.name, location=getattr(b, 'location', None)) # Use getattr for safety
            for b in buckets_iterator
        ]
        logger_bucket.info(f"Found {len(results)} buckets.")
        return results
    except Forbidden as e:
        logger_bucket.error(f"Permission denied listing buckets: {e}", exc_info=True)
        raise HTTPException(status_code=403, detail="Permission denied to list buckets. Check service account roles.")
    except GoogleAPICallError as e:
        logger_bucket.error(f"GCP API error listing buckets: {e}", exc_info=True)
        raise HTTPException(status_code=502, detail=f"Error communicating with Cloud Storage API: {e.message}")
    except Exception as e:
        logger_bucket.error(f"Unexpected error listing buckets: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred while listing buckets.")