"""
Pydantic schemas for chat-related operations
"""

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List, Dict, Any
from datetime import datetime


class ChatMessage(BaseModel):
    """Single chat message"""
    role: str = Field(..., pattern="^(user|assistant)$")
    content: str


class ChatRequest(BaseModel):
    """Chat request"""
    message: str = Field(..., min_length=1, max_length=5000)
    conversation_id: Optional[int] = None
    stream: bool = False
    use_cache: bool = True


class ChatResponse(BaseModel):
    """Chat response"""
    message: str
    conversation_id: int
    sources: Optional[List[Dict[str, Any]]] = None
    confidence_score: Optional[float] = None
    processing_time_ms: Optional[int] = None


class ConversationCreate(BaseModel):
    """Create new conversation"""
    title: Optional[str] = Field(None, max_length=255)


class ConversationResponse(BaseModel):
    """Conversation response"""
    id: int
    user_id: int
    title: Optional[str] = None
    is_archived: bool
    created_at: datetime
    updated_at: Optional[datetime] = None
    message_count: Optional[int] = 0

    model_config = ConfigDict(from_attributes=True)


class MessageResponse(BaseModel):
    """Message response"""
    id: int
    conversation_id: int
    role: str
    content: str
    sources: Optional[List[Dict[str, Any]]] = None
    confidence_score: Optional[float] = None
    processing_time_ms: Optional[int] = None
    rating: Optional[int] = None
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class MessageFeedback(BaseModel):
    """Message feedback"""
    rating: int = Field(..., ge=1, le=5)
    feedback_text: Optional[str] = Field(None, max_length=1000)


class ConversationHistory(BaseModel):
    """Full conversation with messages"""
    conversation: ConversationResponse
    messages: List[MessageResponse]


class FileUploadResponse(BaseModel):
    """File upload response"""
    filename: str
    file_size: int
    content_preview: str
    status: str
