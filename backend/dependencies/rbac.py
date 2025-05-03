# dependencies/rbac.py
from fastapi import Depends, HTTPException, status
import logging

from auth import verify_token # Assuming verify_token returns dict with 'uid'
from services.firestore_service import get_user_role

logger_rbac = logging.getLogger(__name__ + "_rbac")

async def require_admin(user: dict = Depends(verify_token)) -> dict:
    """
    Dependency that requires the user to have the 'admin' role in Firestore.
    """
    user_uid = user.get("uid")
    if not user_uid:
        logger_rbac.error("Could not get UID from verified token.")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate user identity for permission check.",
        )

    user_role = await get_user_role(user_uid)

    if user_role != "admin":
        logger_rbac.warning(f"Permission denied for UID {user_uid}. Role found: '{user_role}'. Required: 'admin'.")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Permission denied: Administrator role required for this action.",
        )

    logger_rbac.info(f"Admin access granted for UID: {user_uid}")
    # Return the original user dict in case downstream needs it,
    # though often just passing the check is sufficient.
    return user