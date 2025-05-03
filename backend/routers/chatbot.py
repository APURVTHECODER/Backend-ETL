# backend/routers/chatbot.py
import os
import logging
from typing import Optional
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
import google.generativeai as genai
from google.api_core.exceptions import GoogleAPICallError, DeadlineExceeded

# Assuming verify_token is correctly defined in your auth module
# If chat doesn't need auth, remove Depends(verify_token)
from auth import verify_token

logger_chatbot = logging.getLogger(__name__ + "_chatbot")
logger_chatbot.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())

# --- Configuration (Ensure Gemini API Key is loaded in main.py) ---
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_CHAT_MODEL = os.getenv("GEMINI_CHAT_MODEL", "gemini-1.5-flash-latest") # Using gemini-pro for general chat
GEMINI_CHAT_TIMEOUT = int(os.getenv("GEMINI_TIMEOUT_SECONDS", 120))

# Check if Gemini was configured successfully in main.py
if not GEMINI_API_KEY:
    logger_chatbot.warning("Chatbot Router: GEMINI_API_KEY not set. Chat endpoint will fail.")
    # No need to configure again if main.py did it.
    # If you want this router to be self-contained, you could add configuration here,
    # but it's better to rely on the central config in main.py.

chat_router = APIRouter(
    prefix="/api/chatbot",
    tags=["Chatbot"],
    dependencies=[Depends(verify_token)] # Apply authentication if needed
)

# --- Pydantic Models ---
class ChatRequest(BaseModel):
    prompt: str = Field(..., min_length=1, description="The user's prompt to the chatbot.")
    # Add history later if needed: history: Optional[List[Dict[str, str]]] = None

class ChatResponse(BaseModel):
    reply: str

# --- Chatbot Endpoint ---
@chat_router.post("/chat", response_model=ChatResponse)
async def handle_chat_prompt(req: ChatRequest):
    """Receives a user prompt and returns a response from the Gemini model."""
    if not GEMINI_API_KEY:
        logger_chatbot.error("Chatbot request failed: Gemini API key not configured.")
        raise HTTPException(status_code=503, detail="Chatbot service is not configured.")

    logger_chatbot.info(f"Received chat prompt: {req.prompt[:100]}...")

    try:
        # Use the Gemini client configured in main.py
        # For simple Q&A, generate_content is fine. For conversational history, use model.start_chat().
        model = genai.GenerativeModel(GEMINI_CHAT_MODEL)
        # You might want to add system instructions or context here if needed
        # e.g., "You are a helpful assistant." + req.prompt
        full_prompt = req.prompt # Keep it simple for now

        response = await model.generate_content_async(
            full_prompt,
            generation_config=genai.types.GenerationConfig(
                # Adjust temperature/top_p as needed for creativity vs factualness
                temperature=0.7,
            ),
            request_options={'timeout': GEMINI_CHAT_TIMEOUT}
        )

        logger_chatbot.debug(f"Gemini Raw Chat Response: {response.text}")

        # Basic safety check (can be enhanced with response.prompt_feedback)
        if not response.text:
            logger_chatbot.warning("Gemini returned an empty response for chat.")
            # Check for safety blocks
            if response.prompt_feedback and response.prompt_feedback.block_reason:
                 reason = response.prompt_feedback.block_reason.name
                 logger_chatbot.warning(f"Chat response blocked due to safety reason: {reason}")
                 raise HTTPException(status_code=400, detail=f"Request blocked due to safety concerns ({reason}). Please rephrase.")
            else:
                 raise HTTPException(status_code=500, detail="AI failed to generate a response.")


        return ChatResponse(reply=response.text.strip())

    except DeadlineExceeded:
        logger_chatbot.error("Gemini API call for chat timed out.")
        raise HTTPException(status_code=504, detail="Chatbot request timed out.")
    except GoogleAPICallError as e:
        logger_chatbot.error(f"Gemini API call error during chat: {e}")
        raise HTTPException(status_code=502, detail=f"Error communicating with AI service: {str(e)}")
    except HTTPException as http_exc:
         # Re-raise specific HTTP exceptions (like the safety block)
         raise http_exc
    except Exception as e:
        logger_chatbot.error(f"Unexpected error during chat processing: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to process chat request: {str(e)}")