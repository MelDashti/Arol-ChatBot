"""
Chat endpoints with RAG and conversation management
"""

from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, desc
from typing import List
import time
import logging

from app.core.database import get_db
from app.core.security import get_current_user
from app.core.ml_model import model_manager
from app.core.rate_limiter import check_rate_limit
from app.models.user import User
from app.models.conversation import Conversation, Message
from app.schemas.chat import (
    ChatRequest,
    ChatResponse,
    ConversationCreate,
    ConversationResponse,
    MessageResponse,
    MessageFeedback,
    ConversationHistory,
    FileUploadResponse
)

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/message", response_model=ChatResponse, dependencies=[Depends(check_rate_limit)])
async def send_message(
    request: ChatRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Send a chat message and get AI response"""

    start_time = time.time()

    # Get or create conversation
    if request.conversation_id:
        result = await db.execute(
            select(Conversation).where(
                Conversation.id == request.conversation_id,
                Conversation.user_id == current_user.id
            )
        )
        conversation = result.scalar_one_or_none()
        if not conversation:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Conversation not found"
            )
    else:
        # Create new conversation
        conversation = Conversation(
            user_id=current_user.id,
            title=request.message[:50] + "..." if len(request.message) > 50 else request.message
        )
        db.add(conversation)
        await db.commit()
        await db.refresh(conversation)

    # Save user message
    user_message = Message(
        conversation_id=conversation.id,
        role="user",
        content=request.message
    )
    db.add(user_message)
    await db.commit()

    # Get AI response
    try:
        ai_response = await model_manager.get_response(
            request.message,
            conversation_id=str(conversation.id),
            use_cache=request.use_cache
        )
    except Exception as e:
        logger.error(f"Error getting AI response: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to generate response"
        )

    processing_time = int((time.time() - start_time) * 1000)

    # Save AI message
    ai_message = Message(
        conversation_id=conversation.id,
        role="assistant",
        content=ai_response,
        processing_time_ms=processing_time
    )
    db.add(ai_message)
    await db.commit()

    return ChatResponse(
        message=ai_response,
        conversation_id=conversation.id,
        processing_time_ms=processing_time
    )


@router.post("/conversations", response_model=ConversationResponse, status_code=status.HTTP_201_CREATED)
async def create_conversation(
    conversation_data: ConversationCreate,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Create a new conversation"""

    conversation = Conversation(
        user_id=current_user.id,
        title=conversation_data.title
    )
    db.add(conversation)
    await db.commit()
    await db.refresh(conversation)

    return conversation


@router.get("/conversations", response_model=List[ConversationResponse])
async def get_conversations(
    skip: int = 0,
    limit: int = 50,
    include_archived: bool = False,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Get user's conversations"""

    query = select(Conversation).where(Conversation.user_id == current_user.id)

    if not include_archived:
        query = query.where(Conversation.is_archived == False)

    query = query.order_by(desc(Conversation.updated_at)).offset(skip).limit(limit)

    result = await db.execute(query)
    conversations = result.scalars().all()

    # Get message counts
    response = []
    for conv in conversations:
        count_result = await db.execute(
            select(func.count(Message.id)).where(Message.conversation_id == conv.id)
        )
        message_count = count_result.scalar()

        conv_dict = ConversationResponse.model_validate(conv).model_dump()
        conv_dict["message_count"] = message_count
        response.append(ConversationResponse(**conv_dict))

    return response


@router.get("/conversations/{conversation_id}", response_model=ConversationHistory)
async def get_conversation(
    conversation_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Get conversation with all messages"""

    # Get conversation
    result = await db.execute(
        select(Conversation).where(
            Conversation.id == conversation_id,
            Conversation.user_id == current_user.id
        )
    )
    conversation = result.scalar_one_or_none()

    if not conversation:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Conversation not found"
        )

    # Get messages
    messages_result = await db.execute(
        select(Message).where(Message.conversation_id == conversation_id).order_by(Message.created_at)
    )
    messages = messages_result.scalars().all()

    return ConversationHistory(
        conversation=ConversationResponse.model_validate(conversation),
        messages=[MessageResponse.model_validate(msg) for msg in messages]
    )


@router.delete("/conversations/{conversation_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_conversation(
    conversation_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Delete a conversation"""

    result = await db.execute(
        select(Conversation).where(
            Conversation.id == conversation_id,
            Conversation.user_id == current_user.id
        )
    )
    conversation = result.scalar_one_or_none()

    if not conversation:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Conversation not found"
        )

    await db.delete(conversation)
    await db.commit()


@router.post("/messages/{message_id}/feedback", status_code=status.HTTP_204_NO_CONTENT)
async def add_message_feedback(
    message_id: int,
    feedback: MessageFeedback,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Add feedback to a message"""

    # Verify message belongs to user's conversation
    result = await db.execute(
        select(Message).join(Conversation).where(
            Message.id == message_id,
            Conversation.user_id == current_user.id
        )
    )
    message = result.scalar_one_or_none()

    if not message:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Message not found"
        )

    message.rating = feedback.rating
    message.feedback_text = feedback.feedback_text
    await db.commit()


@router.post("/upload", response_model=FileUploadResponse)
async def upload_file(
    file: UploadFile = File(...),
    current_user: User = Depends(get_current_user)
):
    """Upload a file for processing (PDF, DOCX, etc.)"""

    from app.core.config import settings
    import os

    # Validate file extension
    file_ext = os.path.splitext(file.filename)[1].lower()
    if file_ext not in settings.ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"File type not allowed. Allowed: {', '.join(settings.ALLOWED_EXTENSIONS)}"
        )

    # Read file
    content = await file.read()
    file_size = len(content)

    if file_size > settings.MAX_UPLOAD_SIZE:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"File too large. Max size: {settings.MAX_UPLOAD_SIZE / 1024 / 1024}MB"
        )

    # Process file content (basic preview)
    # TODO: Implement actual file processing for RAG integration
    content_preview = content[:200].decode('utf-8', errors='ignore') if file_ext == '.txt' else "Binary file"

    return FileUploadResponse(
        filename=file.filename,
        file_size=file_size,
        content_preview=content_preview,
        status="uploaded"
    )
