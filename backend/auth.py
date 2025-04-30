# backend/auth.py
import os
import logging
from typing import Optional, Dict, Any

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import firebase_admin
from firebase_admin import credentials, auth
from dotenv import load_dotenv

# Load environment variables specifically for auth if needed separately
# load_dotenv() # Already loaded in main.py usually
load_dotenv()
logger_auth = logging.getLogger(__name__ + "_auth") # Specific logger for auth

# --- Firebase Admin Initialization ---
SERVICE_ACCOUNT_KEY_PATH = os.getenv("FIREBASE_ADMIN_SDK_KEY_PATH")
FIREBASE_APP_NAME = "ETL-Login" # Unique name if using multiple apps

firebase_app = None

def initialize_firebase_admin():
    """Initializes the Firebase Admin SDK if not already initialized."""
    global firebase_app
    if firebase_admin._apps.get(FIREBASE_APP_NAME):
        logger_auth.debug(f"Firebase app '{FIREBASE_APP_NAME}' already initialized.")
        firebase_app = firebase_admin.get_app(name=FIREBASE_APP_NAME)
        return

    if not SERVICE_ACCOUNT_KEY_PATH:
        logger_auth.error("FIREBASE_ADMIN_SDK_KEY_PATH environment variable not set. Cannot initialize Firebase Admin.")
        raise ValueError("Firebase Admin SDK key path not configured.")

    if not os.path.exists(SERVICE_ACCOUNT_KEY_PATH):
        logger_auth.error(f"Firebase Admin SDK key file not found at: {SERVICE_ACCOUNT_KEY_PATH}")
        raise FileNotFoundError(f"Firebase Admin SDK key file not found at: {SERVICE_ACCOUNT_KEY_PATH}")

    try:
        logger_auth.info(f"Initializing Firebase Admin SDK app '{FIREBASE_APP_NAME}'...")
        cred = credentials.Certificate(SERVICE_ACCOUNT_KEY_PATH)
        firebase_app = firebase_admin.initialize_app(cred, name=FIREBASE_APP_NAME)
        logger_auth.info(f"Firebase Admin SDK app '{FIREBASE_APP_NAME}' initialized successfully.")
    except Exception as e:
        logger_auth.critical(f"Failed to initialize Firebase Admin SDK: {e}", exc_info=True)
        # Decide how to handle this - maybe raise exception to stop FastAPI startup?
        raise # Reraise to prevent app starting without auth

# Call initialization once when the module is loaded
initialize_firebase_admin()

# --- FastAPI Dependency for Authentication ---
oauth2_scheme = HTTPBearer()

async def get_current_user(token: HTTPAuthorizationCredentials = Depends(oauth2_scheme)) -> Dict[str, Any]:
    """
    FastAPI dependency to verify Firebase ID token and return user claims.
    Raises HTTPException 401/403 if token is invalid or missing.
    """
    if not firebase_app:
         logger_auth.error("Firebase Admin SDK not initialized. Cannot verify token.")
         raise HTTPException(
             status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
             detail="Authentication service is unavailable.",
         )

    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        # Verify the ID token while checking if the token is revoked.
        decoded_token = auth.verify_id_token(token.credentials, app=firebase_app, check_revoked=True)
        # You can access user info like:
        # uid = decoded_token.get('uid')
        # email = decoded_token.get('email')
        # logger_auth.debug(f"Token verified successfully for UID: {uid}, Email: {email}")
        return decoded_token # Return the claims dictionary
    except auth.RevokedIdTokenError:
         logger_auth.warning("Authentication failed: ID token has been revoked.")
         raise HTTPException(
             status_code=status.HTTP_401_UNAUTHORIZED,
             detail="Token revoked, please sign in again.",
             headers={"WWW-Authenticate": "Bearer"},
         )
    except auth.UserDisabledError:
        logger_auth.warning("Authentication failed: User account is disabled.")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User account disabled.",
             headers={"WWW-Authenticate": "Bearer"},
        )
    except auth.InvalidIdTokenError as e:
        logger_auth.warning(f"Authentication failed: Invalid ID token: {e}")
        raise credentials_exception
    except Exception as e:
        logger_auth.error(f"Unexpected error during token verification: {e}", exc_info=True)
        raise credentials_exception

# Optional: Dependency that just validates without returning user data
async def verify_token(
    token: HTTPAuthorizationCredentials = Depends(oauth2_scheme)
) -> Dict[str, Any]:
    decoded = auth.verify_id_token(token.credentials, app=firebase_app, check_revoked=True)
    return decoded
