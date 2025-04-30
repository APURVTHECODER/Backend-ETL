# services/firestore_service.py
import firebase_admin
from firebase_admin import credentials, firestore
import logging
import os
from typing import Optional

logger_firestore = logging.getLogger(__name__ + "_firestore")
# db = firestore.client() # <--- REMOVE THIS LINE

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
    cred_path = os.getenv("FIREBASE_ADMIN_SDK_KEY_PATH")
    if not cred_path:
        logger_firestore.error("FIREBASE_ADMIN_SDK_KEY_PATH environment variable not set. Cannot initialize Firebase.")
        db = None
        return

    if not os.path.exists(cred_path):
        logger_firestore.error(f"Firebase Admin SDK key file not found at: {cred_path}")
        db = None
        return

    try:
        logger_firestore.info(f"Initializing Firebase Admin SDK with key: {cred_path}")
        cred = credentials.Certificate(cred_path)
        firebase_admin.initialize_app(cred)
        db = firestore.client()
        logger_firestore.info("Firebase Admin SDK initialized successfully.")
    except Exception as e:
        logger_firestore.critical(f"Failed to initialize Firebase Admin SDK: {e}", exc_info=True)
        db = None



async def get_user_role(user_uid: str) -> Optional[str]:
    """
    Fetches the role for a given Firebase User ID from Firestore.
    Assumes a 'users' collection where document ID is the user_uid.
    Each document should have a 'role' field (e.g., 'admin', 'user').
    """
    if db is None: # Check specifically for None
        logger_firestore.error("Firestore client is None (not initialized or init failed). Cannot fetch user role.")
        # Optionally try re-initializing? Risky in concurrent env. Better to fail.
        # initialize_firestore() # Avoid calling here, should happen at startup
        return None # Or raise an internal server error

    # --- Add extra logging ---
    logger_firestore.info(f"get_user_role called for UID: '{user_uid}'")
    try:
        logger_firestore.debug(f"Using Firestore client project: {db.project}")
        user_doc_ref = db.collection('users').document(user_uid)
        logger_firestore.debug(f"Created doc ref: {user_doc_ref.path}")

        user_doc = user_doc_ref.get()


        logger_firestore.debug(f"Firestore get() result exists: {user_doc.exists}")

        if user_doc.exists:
            user_data = user_doc.to_dict()
            logger_firestore.debug(f"Document data for {user_uid}: {user_data}")
            role = user_data.get('role')
            if role in ['admin', 'user']:
                logger_firestore.info(f"Role found for UID {user_uid}: {role}")
                return role
            else:
                logger_firestore.warning(f"Invalid or missing 'role' field for UID {user_uid}. Data: {user_data}")
                return None
        else:
            logger_firestore.warning(f"User document NOT FOUND in Firestore for UID: '{user_uid}'. Assuming default role.")
            return None
    except Exception as e:
        logger_firestore.error(f"Error during Firestore query for UID {user_uid}: {e}", exc_info=True)
        return None