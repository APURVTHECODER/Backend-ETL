# services/firestore_service.py
import os
import json
import base64
import logging
import firebase_admin
from typing import Optional,List 
from firebase_admin import credentials, firestore
import logging
import os # os might not be needed anymore if not checking path
from typing import Optional

logger_firestore = logging.getLogger(__name__ + "_firestore")
db: Optional[firestore.Client] = None # Initialize db as None, optionally type hint

def initialize_firestore():
    """Initializes the Firebase Admin SDK and Firestore client."""
    global db
    try:
        # Try to get the existing default app
        app = firebase_admin.get_app()
        logger_firestore.info("Firebase Admin SDK already initialized.")
        db = firestore.client(app=app)
        logger_firestore.info("Retrieved Firestore client from existing app.")
        return
    except ValueError:
        logger_firestore.info("Firebase Admin SDK not initialized yet. Proceeding with initialization.")

    # If we're here, need to initialize Firebase
    cred_base64 = os.getenv("FIREBASE_ADMIN_SDK_KEY_BASE64")
    if not cred_base64:
        logger_firestore.error("FIREBASE_ADMIN_SDK_KEY_BASE64 environment variable not set. Cannot initialize Firebase.")
        db = None
        return

    try:
        # Decode the base64-encoded credentials
        decoded_creds = base64.b64decode(cred_base64)
        cred_dict = json.loads(decoded_creds.decode("utf-8"))
        cred = credentials.Certificate(cred_dict)

        logger_firestore.info("Initializing Firebase Admin SDK with base64-decoded credentials.")
        firebase_admin.initialize_app(cred)
        db = firestore.client()
        logger_firestore.info("Firebase Admin SDK initialized successfully.")
    except Exception as e:
        logger_firestore.critical(f"Failed to initialize Firebase Admin SDK with base64 credentials: {e}", exc_info=True)
        db = None
async def get_user_role(user_uid: str) -> Optional[str]:
    """
    Fetches the role for a given Firebase User ID from Firestore.
    Assumes a 'users' collection where document ID is the user_uid.
    Each document should have a 'role' field (e.g., 'admin', 'user').
    """
    global db # Ensure we are using the global db instance

    # Check if initialization was successful (db should not be None)
    if db is None:
        logger_firestore.error("Firestore client is None (initialization failed or not called). Cannot fetch user role.")
        # Attempting re-initialization here is generally bad practice in a request cycle.
        # Ensure initialize_firestore() is called reliably at application startup (e.g., in main.py).
        return None

    # Log the attempt
    logger_firestore.info(f"get_user_role called for UID: '{user_uid}'")
    try:
        # Log the project ID being used by the client
        logger_firestore.debug(f"Using Firestore client project: {db.project}")

        # Get the document reference
        user_doc_ref = db.collection('users').document(user_uid)
        logger_firestore.debug(f"Created doc ref path: {user_doc_ref.path}")

        # Asynchronously get the document snapshot
        # Using user_doc_ref.get() directly works in recent firebase-admin versions with async frameworks like FastAPI
        user_doc = user_doc_ref.get()
        logger_firestore.debug(f"Firestore get() result exists: {user_doc.exists}")

        # Process the document snapshot
        if user_doc.exists:
            user_data = user_doc.to_dict()
            logger_firestore.debug(f"Document data for {user_uid}: {user_data}")
            role = user_data.get('role') # Use .get() for safer access
            if isinstance(role, str) and role in ['admin', 'user']: # Validate type and value
                logger_firestore.info(f"Role found for UID {user_uid}: {role}")
                return role
            else:
                logger_firestore.warning(f"Invalid or missing 'role' field (or wrong type) for UID {user_uid}. Data: {user_data}")
                # Decide default behavior: return 'user' or None? Returning None forces explicit assignment.
                return None
        else:
            logger_firestore.warning(f"User document NOT FOUND in Firestore for UID: '{user_uid}'. Assuming default role (None).")
            return None # No document means no specific role assigned
    except Exception as e:
        # Catch any unexpected errors during the Firestore interaction
        logger_firestore.error(f"Error during Firestore query for UID {user_uid}: {e}", exc_info=True)
        return None # Indicate error fetching role
    
async def get_user_accessible_datasets(user_uid: str) -> Optional[List[str]]:
    """
    Fetches the list of dataset IDs a specific user can access from Firestore.
    Returns a list of dataset IDs, an empty list, or None on error/not found.
    """
    global db
    if db is None:
        logger_firestore.error("Firestore client is None. Cannot fetch accessible datasets.")
        return None

    logger_firestore.info(f"Fetching accessible datasets for UID: '{user_uid}'")
    try:
        user_doc_ref = db.collection('users').document(user_uid)
        user_doc =  user_doc_ref.get() # Use await

        if user_doc.exists:
            user_data = user_doc.to_dict()
            datasets = user_data.get('accessible_datasets')

            # IMPORTANT: Check if it's a list of strings
            if isinstance(datasets, list) and all(isinstance(item, str) for item in datasets):
                logger_firestore.info(f"Accessible datasets found for UID {user_uid}: {len(datasets)} dataset(s)")
                return datasets
            elif datasets is None:
                 logger_firestore.info(f"No 'accessible_datasets' field found for UID {user_uid}. Returning empty list.")
                 return [] # Treat missing field as no access explicitly granted
            else:
                logger_firestore.warning(f"Invalid 'accessible_datasets' field (not a list of strings) for UID {user_uid}. Data: {datasets}. Returning empty list.")
                return [] # Treat invalid data as no access explicitly granted
        else:
            logger_firestore.warning(f"User document NOT FOUND for UID: '{user_uid}' when fetching datasets. Returning None.")
            return None # No document means no specific permissions known
    except Exception as e:
        logger_firestore.error(f"Error fetching accessible datasets for UID {user_uid}: {e}", exc_info=True)
        return None