# routers/user_profile.py
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
import logging

from auth import verify_token
from services.firestore_service import get_user_role

user_profile_router = APIRouter(
    prefix="/api/users",
    tags=["User Profile"],
    dependencies=[Depends(verify_token)] # Protect all routes in this router
)

logger_profile = logging.getLogger(__name__ + "_user_profile")

class UserProfileResponse(BaseModel):
    user_id: str = Field(..., description="Firebase User ID (UID)")
    role: str = Field(default='user', description="User's assigned role ('admin' or 'user')")
    # Add other profile fields if needed (e.g., email, name)

@user_profile_router.get("/me/profile", response_model=UserProfileResponse)
async def get_my_profile(user: dict = Depends(verify_token)):
    """
    Retrieves the profile information (including role) for the currently authenticated user.
    """
    user_uid = user.get("uid")
    if not user_uid:
        # This shouldn't happen if verify_token works, but check defensively
        logger_profile.error("UID missing from token after verification in /me/profile.")
        raise HTTPException(
             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
             detail="Could not identify user.",
        )

    # Fetch role from Firestore
    role_from_db = await get_user_role(user_uid)

    # Default to 'user' if not found in DB or if fetch fails, for robustness.
    # The RBAC dependency enforces strict checks; this endpoint is informational.
    effective_role = role_from_db if role_from_db else 'user'

    logger_profile.info(f"Returning profile for UID {user_uid} with effective role: {effective_role}")

    return UserProfileResponse(
        user_id=user_uid,
        role=effective_role
        # Include other details from the 'user' dict if available and needed
        # email=user.get("email")
    )