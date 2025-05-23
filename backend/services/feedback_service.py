# services/feedback_service.py
import logging
from typing import Dict, Any, Optional
from firebase_admin import firestore
import uuid
from .firestore_service import get_firestore_client, logger_firestore # Adjust if needed
# Assuming db is initialized and accessible similar to firestore_service.py
# You might want to pass `db` or import it from a central place.
# For this example, let's assume it's imported from your existing firestore_service
from .firestore_service import db, logger_firestore # Adjust import path if needed

logger_feedback = logging.getLogger(__name__ + "_feedback")

async def store_feedback(
    user_uid: str,
    feedback_data: Dict[str, Any]
) -> Optional[str]:
    """
    Stores user feedback into a Firestore collection.
    """
    current_db = get_firestore_client() # <<<< GET THE CLIENT HERE
    if current_db is None:
        logger_feedback.error("Firestore client is None (obtained via getter). Cannot store feedback.")
        return None

    try:
        feedback_id = str(uuid.uuid4())
        # Use current_db
        feedback_doc_ref = current_db.collection('user_feedback').document(feedback_id)

        full_feedback_payload = {
            "feedbackId": feedback_id,
            "userId": user_uid,
            "timestamp": firestore.SERVER_TIMESTAMP,
            **feedback_data,
            "status": "NEW",
        }

        feedback_doc_ref.set(full_feedback_payload)
        logger_feedback.info(f"Feedback {feedback_id} from user {user_uid} stored successfully.")
        return feedback_id
    except Exception as e:
        logger_feedback.error(f"Error storing feedback for user {user_uid}: {e}", exc_info=True)
        return None