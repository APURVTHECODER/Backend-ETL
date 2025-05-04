# backend/auth.py

import os
import logging
import base64 # +++ Import base64
import json   # +++ Import json

from typing import Optional, Dict, Any

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import firebase_admin
from firebase_admin import credentials, auth
from dotenv import load_dotenv

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

async def get_current_user(token: HTTPAuthorizationCredentials = Depends(oauth2_scheme)) -> Dict[str, Any]:
    """ FastAPI dependency to verify Firebase ID token and return user claims. """
    if not firebase_app:
         logger_auth.error("Firebase Admin SDK not initialized (get_current_user). Cannot verify token.")
         raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Auth service unavailable.")

    credentials_exception = HTTPException( status_code=status.HTTP_401_UNAUTHORIZED, detail="Could not validate credentials", headers={"WWW-Authenticate": "Bearer"}, )
    try:
        decoded_token = auth.verify_id_token(token.credentials, app=firebase_app, check_revoked=True)
        return decoded_token
    except auth.RevokedIdTokenError:
         logger_auth.warning("Auth failed: Token revoked.")
         raise HTTPException( status_code=status.HTTP_401_UNAUTHORIZED, detail="Token revoked.", headers={"WWW-Authenticate": "Bearer"}, )
    except auth.UserDisabledError:
        logger_auth.warning("Auth failed: User disabled.")
        raise HTTPException( status_code=status.HTTP_403_FORBIDDEN, detail="User disabled.", headers={"WWW-Authenticate": "Bearer"}, )
    except auth.InvalidIdTokenError as e:
        logger_auth.warning(f"Auth failed: Invalid token: {e}")
        raise credentials_exception
    except Exception as e:
        logger_auth.error(f"Unexpected error during token verification: {e}", exc_info=True)
        raise credentials_exception

# --- verify_token function ---
# Ensure firebase_app is checked here too, or rely on get_current_user's check if always used together
async def verify_token( token: HTTPAuthorizationCredentials = Depends(oauth2_scheme) ) -> Dict[str, Any]:
    """ Verifies token, used as dependency. """
    if not firebase_app:
        logger_auth.error("Firebase Admin SDK not initialized (verify_token). Cannot verify token.")
        raise HTTPException(status_code=503, detail="Auth service unavailable.")
    try:
        # Reuse logic/error handling from get_current_user if possible
        # This is simplified for brevity, add full error handling as above if needed separately
        decoded = auth.verify_id_token(token.credentials, app=firebase_app, check_revoked=True)
        return decoded
    except Exception as e:
         logger_auth.warning(f"Token verification failed in verify_token: {e}")
         raise HTTPException( status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token", headers={"WWW-Authenticate": "Bearer"}, )