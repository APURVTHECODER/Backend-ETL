# routers/user_profile.py
from fastapi import APIRouter, Depends, HTTPException, status
# Added EmailStr, validator, List, Optional, Dict, Any for the new endpoint
from pydantic import BaseModel, Field, EmailStr, validator
import logging
# Added List, Optional, Dict, Any for the new endpoint
from typing import List, Optional, Dict, Any
# Added asyncio for the new endpoint
import asyncio

from auth import verify_token
# Added set_user_access, get_uid_from_email for the new endpoint
from services.firestore_service import get_user_role, set_user_access, get_uid_from_email
# Added require_admin for the new endpoint
from dependencies.rbac import require_admin

user_profile_router = APIRouter(
    prefix="/api/users",
    tags=["User Profile & Access Management"], # Updated tag to reflect added functionality
    dependencies=[Depends(verify_token)] # Protect all routes in this router
)

logger_profile = logging.getLogger(__name__ + "_user_profile")

# --- Existing Model (No Changes) ---
class UserProfileResponse(BaseModel):
    user_id: str = Field(..., description="Firebase User ID (UID)")
    role: str = Field(default='user', description="User's assigned role ('admin' or 'user')")
    # Add other profile fields if needed (e.g., email, name)

# --- Models for the New Endpoint ---
class ManageAccessRequest(BaseModel):
    emails: List[EmailStr] = Field(..., description="List of user email addresses to manage.")
    role: str = Field(..., description="The role to assign ('admin' or 'user').")
    dataset_ids: Optional[List[str]] = Field(None, description="List of dataset IDs to grant access to. Required if role is 'user', ignored if 'admin'.")

    @validator('role')
    def validate_role(cls, v):
        if v not in ['admin', 'user']:
            raise ValueError("Role must be either 'admin' or 'user'")
        return v

    @validator('dataset_ids', always=True)
    def check_dataset_ids_for_user_role(cls, v, values):
        role = values.get('role')
        if role == 'user' and v is None:
             # Allow None, service layer treats as empty list []
             pass
        if role == 'admin' and v is not None:
            logger_profile.warning("dataset_ids provided for admin role, they will be ignored.")
            # Optional: return None # To ensure it's cleared if logic depends on it being None
        return v

class ManageAccessResponse(BaseModel):
    processed_count: int = Field(..., description="Number of emails successfully processed.")
    success_count: int = Field(..., description="Number of users whose access was successfully updated/set.")
    failed_details: List[Dict[str, str]] = Field(..., description="List of emails that failed, with reasons.")
    message: str = Field(..., description="Summary message of the operation.")


# --- Existing Endpoint (No Changes) ---
@user_profile_router.get("/me/profile", response_model=UserProfileResponse)
async def get_my_profile(user: dict = Depends(verify_token)):
    """
    Retrieves the profile information (including role) for the currently authenticated user.
    """
    user_uid = user.get("uid")
    if not user_uid:
        logger_profile.error("UID missing from token after verification in /me/profile.")
        raise HTTPException(
             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
             detail="Could not identify user.",
        )
    role_from_db = await get_user_role(user_uid)
    effective_role = role_from_db if role_from_db else 'user'
    logger_profile.info(f"Returning profile for UID {user_uid} with effective role: {effective_role}")
    return UserProfileResponse(
        user_id=user_uid,
        role=effective_role
    )

# --- New Endpoint (Added) ---
@user_profile_router.post(
    "/manage-access",
    response_model=ManageAccessResponse,
    dependencies=[Depends(require_admin)], # Ensure only admins can call this
    summary="Manage User Roles and Dataset Access (Admin Only)",
    status_code=status.HTTP_200_OK # Return 200 even for partial success
)
async def manage_user_access(
    request: ManageAccessRequest,
    admin_user: dict = Depends(require_admin) # Get admin user data for logging
):
    """
    Allows an administrator to set the role and accessible datasets for multiple users
    based on their email addresses. Creates user records in Firestore if they don't exist
    but the user exists in Firebase Auth.
    """
    admin_uid = admin_user.get("uid", "unknown_admin")
    logger_profile.info(f"Admin {admin_uid} initiating manage access request for {len(request.emails)} emails. Role: {request.role}")

    if not request.emails:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Email list cannot be empty.")

    success_updates = 0
    failed_details: List[Dict[str, str]] = []

    # Process emails concurrently
    async def process_email(email: str):
        nonlocal success_updates # Allow modification of outer scope variable
        uid = await get_uid_from_email(email)
        if uid:
            # Pass dataset_ids only if role is 'user'
            datasets = request.dataset_ids if request.role == 'user' else None
            success = await set_user_access(uid, request.role, datasets)
            if success:
                 # Only increment if Firestore update was successful
                 success_updates += 1
                 return None # Indicate success for this email
            else:
                # Firestore update failed for this UID
                return {"email": email, "reason": "Failed to update Firestore record"}
        else:
            # UID lookup failed for this email
            return {"email": email, "reason": "User not found in Firebase Authentication"}

    # Run tasks concurrently using asyncio.gather
    tasks = [process_email(email) for email in request.emails]
    results = await asyncio.gather(*tasks)

    # Collect failures from results (None indicates success)
    for result in results:
        if result is not None:
            failed_details.append(result)

    processed_count = len(request.emails)
    # 'success_updates' counter reflects actual successful Firestore writes
    final_success_count = success_updates

    # Construct response message
    if not failed_details:
        message = f"Successfully updated access for all {processed_count} email(s)."
    elif final_success_count > 0:
        message = f"Processed {processed_count} email(s). Successfully updated {final_success_count}. Failed {len(failed_details)} (see details)."
    else: # All failed
        message = f"Failed to update access for all {processed_count} provided email(s)."

    logger_profile.info(f"Manage access result for admin {admin_uid}: {message}")

    return ManageAccessResponse(
        processed_count=processed_count,
        success_count=final_success_count,
        failed_details=failed_details,
        message=message
    )