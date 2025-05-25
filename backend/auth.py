# backend/auth.py

import os
import logging
import base64 # +++ Import base64
import json   # +++ Import json

from typing import Optional, Dict, Any

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import firebase_admin
from firebase_admin import credentials, auth ,exceptions
from dotenv import load_dotenv
from services.firestore_service import ensure_user_document_exists


load_dotenv() # Load .env if present locally
logger_auth = logging.getLogger(__name__ + "_auth")

# --- Firebase Admin Initialization ---
# Read the ENV VAR - now expecting Base64 encoded JSON content, not a path
ENCODED_SERVICE_ACCOUNT_KEY = os.getenv("FIREBASE_ADMIN_SDK_KEY_BASE64")
FIREBASE_APP_NAME = "ETL-Login" # Unique name if using multiple apps

firebase_app = None

def initialize_firebase_admin():
    """
    Initializes the Firebase Admin SDK using a Base64 encoded key
    from the FIREBASE_ADMIN_SDK_KEY_BASE64 environment variable.
    """
    global firebase_app

    # Check if the specific named app is already initialized
    try:
        # Use get_app which raises ValueError if not found
        firebase_app = firebase_admin.get_app(name=FIREBASE_APP_NAME)
        logger_auth.info(f"Firebase app '{FIREBASE_APP_NAME}' already initialized.")
        return
    except ValueError:
        logger_auth.info(f"Firebase app '{FIREBASE_APP_NAME}' not yet initialized. Initializing now.")
        pass # Continue with initialization

    if not ENCODED_SERVICE_ACCOUNT_KEY:
        logger_auth.error("FIREBASE_ADMIN_SDK_KEY_BASE64 environment variable not set or empty. Cannot initialize Firebase Admin.")
        raise ValueError("Firebase Admin SDK key (Base64 content) not configured.")

    try:
        logger_auth.info(f"Decoding Base64 key and initializing Firebase Admin SDK app '{FIREBASE_APP_NAME}'...")

        # +++ Decode Base64 and Parse JSON +++
        try:
            decoded_key_bytes = base64.b64decode(ENCODED_SERVICE_ACCOUNT_KEY)
            decoded_key_str = decoded_key_bytes.decode('utf-8')
            key_dict = json.loads(decoded_key_str)
        except (base64.binascii.Error, json.JSONDecodeError, UnicodeDecodeError) as decode_error:
            logger_auth.error(f"Failed to decode/parse Firebase key from environment variable: {decode_error}")
            raise ValueError("Invalid Firebase Admin SDK key format in environment variable.") from decode_error
        # +++ End Decode/Parse +++

        # Initialize using the key dictionary
        cred = credentials.Certificate(key_dict)
        firebase_app = firebase_admin.initialize_app(cred, name=FIREBASE_APP_NAME)
        logger_auth.info(f"Firebase Admin SDK app '{FIREBASE_APP_NAME}' initialized successfully from decoded key.")

    except Exception as e:
        logger_auth.critical(f"Failed to initialize Firebase Admin SDK: {e}", exc_info=True)
        raise # Reraise to prevent app starting without auth

# Call initialization once when the module is loaded
# Ensure this doesn't conflict with initialization in main.py if done there too
# Best practice is ONE central initialization point. Assuming auth.py is it for now based on traceback.
initialize_firebase_admin()

# --- FastAPI Dependency for Authentication ---
oauth2_scheme = HTTPBearer()

def get_current_user(token_cred: HTTPAuthorizationCredentials = Depends(oauth2_scheme)) -> Dict[str, Any]:
    """ 
    FastAPI dependency to verify Firebase ID token, ensure Firestore user document, 
    and return user claims. 
    """
    if not firebase_app:
         logger_auth.error("Firebase Admin SDK not initialized (get_current_user). Cannot verify token.")
         raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Auth service unavailable.")

    if not token_cred or not token_cred.credentials: # Check if token_cred and credentials exist
        logger_auth.warning("No token credentials provided for verification.")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated: No token provided.",
            headers={"WWW-Authenticate": "Bearer error=\"invalid_request\""}, # More specific header
        )

    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        # Verify the token
        decoded_token = auth.verify_id_token(token_cred.credentials, app=firebase_app, check_revoked=True)
        
        # Extract user details from token
        uid = decoded_token.get("uid")
        email = decoded_token.get("email")
        # Firebase often puts display name in 'name', 'displayName' might also exist from custom claims
        display_name = decoded_token.get("name") or decoded_token.get("display_name") 

        if not uid:
            logger_auth.error("Token decoded but UID is missing.")
            raise credentials_exception # Re-use the generic 401

        # +++ ENSURE FIRESTORE DOCUMENT FOR THE AUTHENTICATED USER +++
        logger_auth.info(f"Token verified for UID: {uid}. Ensuring Firestore document.")
        doc_ensured = ensure_user_document_exists(uid, email=email, display_name=display_name)
        
        if not doc_ensured:
            # This is a critical failure if we can't even ensure the doc.
            logger_auth.error(f"CRITICAL: Failed to ensure Firestore document for authenticated user UID: {uid}")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE, 
                detail="User profile initialization failed. Please try again later.",
            )
        logger_auth.info(f"Firestore document ensured for UID: {uid}.")
        # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
        
        return decoded_token # Return the full decoded token

    except auth.RevokedIdTokenError:
         logger_auth.warning(f"Auth failed for UID {decoded_token.get('uid', 'unknown') if 'decoded_token' in locals() else 'unknown'}: Token revoked.")
         raise HTTPException(
             status_code=status.HTTP_401_UNAUTHORIZED,
             detail="Token has been revoked.",
             headers={"WWW-Authenticate": "Bearer error=\"invalid_token\", error_description=\"The token has been revoked.\""},
         )
    except auth.UserDisabledError:
        logger_auth.warning(f"Auth failed for UID {decoded_token.get('uid', 'unknown') if 'decoded_token' in locals() else 'unknown'}: User disabled.")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, # 403 is more appropriate for a disabled user
            detail="User account has been disabled.",
            # No WWW-Authenticate header for 403 generally
        )
    except auth.InvalidIdTokenError as e: # Catches various token format/signature/expiry issues
        logger_auth.warning(f"Auth failed: Invalid ID token: {e}")
        raise HTTPException(
             status_code=status.HTTP_401_UNAUTHORIZED,
             detail=f"Invalid token: {str(e)}", # Provide a bit more info from the exception
             headers={"WWW-Authenticate": "Bearer error=\"invalid_token\""},
         )
    except exceptions.FirebaseError as e: # Catch other Firebase Admin SDK errors
        logger_auth.error(f"Firebase Admin SDK error during token verification: {e}", exc_info=True)
        raise credentials_exception # Fallback to generic 401
    except HTTPException as http_exc: # Re-raise HTTPExceptions from ensure_user_document_exists
        raise http_exc
    except Exception as e: # Catch-all for truly unexpected errors
        logger_auth.error(f"Unexpected error during token verification or user doc ensuring: {e}", exc_info=True)
        # For unexpected errors, a 500 might be more appropriate than a 401
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An internal error occurred during authentication processing.")

# --- verify_token function ---
# Ensure firebase_app is checked here too, or rely on get_current_user's check if always used together
def verify_token(current_user_data: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    """ 
    Verifies token and ensures user document. Used as a primary dependency for protected routes.
    Relies on get_current_user for the actual verification and Firestore document ensuring.
    """
    # The heavy lifting (token verification and Firestore doc ensuring) is done by get_current_user.
    # This function now mostly serves as a clear dependency name for your routes.
    return current_user_data