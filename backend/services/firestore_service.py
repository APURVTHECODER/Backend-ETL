# services/firestore_service.py
import os
import json
import base64
import logging
import firebase_admin
from typing import Dict, Optional,List 
from firebase_admin import credentials, firestore, auth
import logging
import os # os might not be needed anymore if not checking path
from typing import Optional
import asyncio # For potential concurrency later


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
    


async def set_user_access(user_uid: str, role: str, dataset_ids: Optional[List[str]]) -> bool:
    """
    Sets or updates the role and accessible datasets for a user in Firestore.
    Creates the user document if it doesn't exist. Uses merge=True.

    Args:
        user_uid: The Firebase UID of the user.
        role: The role to assign ('admin' or 'user').
        dataset_ids: A list of dataset IDs if role is 'user', otherwise ignored.

    Returns:
        True if the operation was successful, False otherwise.
    """
    global db
    if db is None:
        logger_firestore.error(f"Firestore client is None. Cannot set access for UID: {user_uid}")
        return False

    if role not in ['admin', 'user']:
        logger_firestore.error(f"Invalid role '{role}' provided for UID: {user_uid}")
        return False

    logger_firestore.info(f"Setting access for UID: {user_uid} - Role: {role}")

    try:
        user_doc_ref = db.collection('users').document(user_uid)
        data_to_set = {'role': role}

        if role == 'user':
            data_to_set['accessible_datasets'] = dataset_ids if dataset_ids else []
            logger_firestore.info(f"Assigning {len(data_to_set['accessible_datasets'])} datasets to user {user_uid}")
        else:
            logger_firestore.info(f"Assigning admin role to {user_uid}. Dataset list assignment skipped.")
            # Optional: data_to_set['accessible_datasets'] = firestore.DELETE_FIELD

        # Use set with merge=True to create or update the document non-destructively
        # --- REMOVED 'await' FROM THE NEXT LINE ---
        user_doc_ref.set(data_to_set, merge=True)

        logger_firestore.info(f"Successfully set access for UID: {user_uid}")
        return True

    except Exception as e:
        logger_firestore.error(f"Error setting access for UID {user_uid}: {e}", exc_info=True)
        return False


logger_firestore = logging.getLogger(__name__ + "_firestore") # Ensure logger is defined


async def get_uid_from_email(email: str) -> Optional[str]:
    """
    Finds the Firebase UID associated with an email address.

    Args:
        email: The email address to look up.

    Returns:
        The UID string if found, None otherwise.
    """
    try:
        # Ensure SDK is initialized! Add check if needed:
        # if not firebase_admin._apps:
        #     logger_firestore.critical("Firebase Admin SDK not initialized!")
        #     return None # Or raise an exception

        user = auth.get_user_by_email(email)
        logger_firestore.info(f"Found UID {user.uid} for email {email}")
        return user.uid
    except auth.UserNotFoundError:
        # This specific exception seems correct, keep it.
        logger_firestore.warning(f"Firebase Auth: User not found for email: {email}")
        return None
    except Exception as e: # <--- CATCH ANY OTHER EXCEPTION HERE
        # Log the specific type of error that occurred and the message
        logger_firestore.error(
            f"Error looking up email {email}. Type: {type(e).__name__}, Error: {e}",
            exc_info=True # Include traceback in logs
        )
        return None
    
async def register_workspace_and_grant_access(
    dataset_id: str,
    owner_uid: str,
    location: str,
    description: Optional[str] = None,
    labels: Optional[Dict[str, str]] = None
) -> bool:
    """
    1. Creates a record for the new workspace in the 'workspaces' (or 'datasets') collection,
       marking the owner and setting initial accessible_users to the owner.
    2. Adds the new workspace ID to the owner's 'accessible_datasets' list in their
       'users' collection document.
    """
    global db
    if db is None:
        logger_firestore.error("Firestore client is None. Cannot register workspace.")
        return False

    # Use a consistent collection name for workspace/dataset metadata.
    # Let's assume 'workspaces' for this example. If you use 'datasets', adjust accordingly.
    WORKSPACE_METADATA_COLLECTION = 'workspaces' # Or 'datasets' if that's what you use

    try:
        # --- Step 1: Create/Update the workspace document in WORKSPACE_METADATA_COLLECTION ---
        workspace_doc_ref = db.collection(WORKSPACE_METADATA_COLLECTION).document(dataset_id)
        workspace_data = {
            'owner_uid': owner_uid,
            'location': location,
            'created_at': firestore.SERVER_TIMESTAMP,
            'accessible_users': [owner_uid] # Initially, only the owner has explicit access listed here
        }
        if description:
            workspace_data['description'] = description
        if labels:
            workspace_data['labels'] = labels

        # Using set() will create if not exists, or overwrite if it does.
        # If you need to prevent overwriting, you might use .create() and handle exceptions.
        workspace_doc_ref.set(workspace_data)
        logger_firestore.info(
            f"Workspace metadata record created/updated for '{dataset_id}' in '{WORKSPACE_METADATA_COLLECTION}' with owner '{owner_uid}'."
        )

        # --- Step 2: Add this dataset_id to the owner's 'accessible_datasets' list in 'users' collection ---
        user_doc_ref = db.collection('users').document(owner_uid)

        # Atomically add the new dataset_id to the 'accessible_datasets' array.
        # This also creates the 'accessible_datasets' field if it doesn't exist.
        user_doc_ref.update({
            'accessible_datasets': firestore.ArrayUnion([dataset_id])
        })
        logger_firestore.info(f"Added '{dataset_id}' to 'accessible_datasets' for user '{owner_uid}'.")

        return True
    except Exception as e:
        logger_firestore.error(
            f"Error in register_workspace_and_grant_access for workspace '{dataset_id}', owner '{owner_uid}': {e}",
            exc_info=True
        )
        # Consider more sophisticated rollback if step 2 fails after step 1 succeeded.
        # For simplicity, we're not implementing full transactional rollback across collections here.
        return False


async def remove_workspace_from_firestore(dataset_id: str) -> bool:
    """
    Removes a workspace from Firestore:
    1. Deletes the workspace's metadata document (e.g., from 'workspaces/{dataset_id}').
    2. Removes the dataset_id from the 'accessible_datasets' array of all users
       who had access to it.
    """
    global db
    if db is None:
        logger_firestore.error(f"Firestore client is None. Cannot remove workspace '{dataset_id}'.")
        return False

    logger_firestore.info(f"Attempting to remove workspace '{dataset_id}' and its references from Firestore.")
    
    WORKSPACE_METADATA_COLLECTION = 'workspaces' # Or 'datasets' if that's your metadata collection name

    batch = db.batch()
    success = True

    try:
        # Step 1: Delete the workspace's metadata document
        workspace_doc_ref = db.collection(WORKSPACE_METADATA_COLLECTION).document(dataset_id)
        # Check if it exists before trying to delete to avoid error if already gone
        workspace_doc =  workspace_doc_ref.get()
        if workspace_doc.exists:
            batch.delete(workspace_doc_ref)
            logger_firestore.info(f"Scheduled deletion for workspace metadata document: '{WORKSPACE_METADATA_COLLECTION}/{dataset_id}'.")
        else:
            logger_firestore.warning(f"Workspace metadata document '{WORKSPACE_METADATA_COLLECTION}/{dataset_id}' not found for deletion.")

        # Step 2: Remove the dataset_id from all users' accessible_datasets arrays
        users_ref = db.collection('users')
        # Query for users who have this dataset_id in their accessible_datasets array
        users_with_access_query = users_ref.where('accessible_datasets', 'array_contains', dataset_id)
        
        # Firestore transactions or batches are better for multiple writes.
        # For simplicity with async, we'll iterate and update, but batching is preferred for >500 docs.
        docs_stream = users_with_access_query.stream() # Use stream for async iteration
        for user_doc_snapshot in docs_stream:
            logger_firestore.info(f"Found user '{user_doc_snapshot.id}' with access to '{dataset_id}'. Scheduling removal.")
            user_doc_ref = users_ref.document(user_doc_snapshot.id)
            # Atomically remove the dataset_id from the array
            batch.update(user_doc_ref, {'accessible_datasets': firestore.ArrayRemove([dataset_id])})
        
        batch.commit() # Commit all batched operations
        logger_firestore.info(f"Successfully committed Firestore operations for removing workspace '{dataset_id}'.")
        return True

    except Exception as e:
        logger_firestore.error(f"Error removing workspace '{dataset_id}' from Firestore: {e}", exc_info=True)
        # If batch commit fails, individual operations might or might not have succeeded.
        # This part is tricky to make fully atomic without transactions spanning collections (which Firestore doesn't easily do).
        return False
    
# services/firestore_service.py
# ... (existing imports and code) ...

async def ensure_user_document_exists(user_uid: str, email: Optional[str] = None, display_name: Optional[str] = None) -> bool:
    """
    Checks if a user document exists in Firestore for the given UID.
    If not, it creates a new document with default values.

    Args:
        user_uid: The Firebase UID of the user.
        email: The user's email (optional, but good to store).
        display_name: The user's display name (optional).

    Returns:
        True if the document exists or was successfully created, False on error.
    """
    global db
    if db is None:
        logger_firestore.error(f"Firestore client is None. Cannot ensure user document for UID: {user_uid}")
        return False

    logger_firestore.info(f"Ensuring Firestore document exists for UID: {user_uid}")
    user_doc_ref = db.collection('users').document(user_uid)

    try:
        user_doc =  user_doc_ref.get() # Use await for async get()

        if user_doc.exists:
            logger_firestore.info(f"User document already exists for UID: {user_uid}. Verifying essential fields.")
            # Optionally, verify and update if essential fields like 'role' or 'accessible_datasets' are missing
            # This can happen if the schema changed or an older document exists without these fields.
            user_data = user_doc.to_dict()
            updates_needed = {}
            if 'role' not in user_data:
                updates_needed['role'] = 'user'
                logger_firestore.warning(f"User {user_uid} document exists but missing 'role'. Setting to default 'user'.")
            if 'accessible_datasets' not in user_data:
                updates_needed['accessible_datasets'] = []
                logger_firestore.warning(f"User {user_uid} document exists but missing 'accessible_datasets'. Setting to empty list.")
            
            if updates_needed:
                user_doc_ref.update(updates_needed) # Use await for async update()
                logger_firestore.info(f"Updated missing essential fields for user {user_uid}.")
            return True
        else:
            logger_firestore.info(f"User document NOT FOUND for UID: {user_uid}. Creating with default values.")
            default_user_data = {
                'uid': user_uid,
                'role': 'user',  # Default role
                'accessible_datasets': [],  # Default empty list
                'createdAt': firestore.SERVER_TIMESTAMP,
            }
            if email:
                default_user_data['email'] = email
            if display_name:
                default_user_data['displayName'] = display_name
            
            user_doc_ref.set(default_user_data) # Use await for async set()
            logger_firestore.info(f"Successfully created user document for UID: {user_uid} with defaults.")
            return True

    except Exception as e:
        logger_firestore.error(f"Error ensuring/creating user document for UID {user_uid}: {e}", exc_info=True)
        return False


# +++ ADD THIS FUNCTION +++
def get_firestore_client() -> Optional[firestore.Client]:
    """Returns the initialized Firestore client instance."""
    if db is None:
        logger_firestore.error("Firestore client requested before initialization or initialization failed.")
    return db
# ... (rest of your firestore_service.py code) ...