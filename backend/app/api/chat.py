"""Chat endpoints: message and history."""

import logging
import uuid

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from app.core.deps import get_redis_client
from app.models.schemas import ChatMessageRequest, ChatMessageResponse
from app.services.chat_service import ChatService

logger = logging.getLogger(__name__)
router = APIRouter()

# Initialize chat service with Redis client (graceful fallback if unavailable)
try:
    _redis = get_redis_client()
except Exception:
    logger.warning("Redis unavailable, chat sessions will be in-memory only")
    _redis = None

chat_service = ChatService(redis_client=_redis)

# --- Constants ---
MAX_MESSAGE_LENGTH: int = 500


@router.post("/message")
async def send_message(req: ChatMessageRequest) -> JSONResponse:
    """Send a chat message, receive JSON response."""
    if not req.message or not req.message.strip():
        raise HTTPException(status_code=400, detail="消息不能为空")

    if len(req.message) > MAX_MESSAGE_LENGTH:
        raise HTTPException(
            status_code=400,
            detail=f"消息长度不能超过{MAX_MESSAGE_LENGTH}字符",
        )

    session_id = req.session_id or str(uuid.uuid4())

    response = await chat_service.process_message(session_id, req.message)

    result: dict = {
        "session_id": session_id,
        "reply": response.reply,
    }

    if response.plans:
        result["plans"] = [p.model_dump() for p in response.plans]
        result["actions"] = ["select_a", "select_b", "reject"]

    return JSONResponse(content=result)


@router.get("/history/{session_id}")
async def get_history(session_id: str) -> list[ChatMessageResponse]:
    """Get chat history for a session."""
    history = await chat_service.get_history(session_id)
    return [
        ChatMessageResponse(role=msg["role"], content=msg["content"])
        for msg in history
    ]
