"""
Conversation and message database models
"""

from sqlalchemy import Column, Integer, String, Text, DateTime, ForeignKey, Boolean, Float, JSON
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship

from app.core.database import Base


class Conversation(Base):
    """Conversation model"""

    __tablename__ = "conversations"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    title = Column(String(255), nullable=True)
    is_archived = Column(Boolean, default=False, nullable=False)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), nullable=True)

    # Relationships
    user = relationship("User", back_populates="conversations")
    messages = relationship("Message", back_populates="conversation", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Conversation {self.id}>"


class Message(Base):
    """Message model"""

    __tablename__ = "messages"

    id = Column(Integer, primary_key=True, index=True)
    conversation_id = Column(Integer, ForeignKey("conversations.id", ondelete="CASCADE"), nullable=False)

    role = Column(String(20), nullable=False)  # 'user' or 'assistant'
    content = Column(Text, nullable=False)

    # Metadata
    sources = Column(JSON, nullable=True)  # Retrieved documents
    confidence_score = Column(Float, nullable=True)
    processing_time_ms = Column(Integer, nullable=True)
    tokens_used = Column(Integer, nullable=True)

    # Feedback
    rating = Column(Integer, nullable=True)  # 1-5 stars or thumbs up/down
    feedback_text = Column(Text, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    # Relationships
    conversation = relationship("Conversation", back_populates="messages")

    def __repr__(self):
        return f"<Message {self.id} - {self.role}>"


class APIKey(Base):
    """API Key model for programmatic access"""

    __tablename__ = "api_keys"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)

    key_hash = Column(String(255), unique=True, index=True, nullable=False)
    name = Column(String(100), nullable=False)
    description = Column(Text, nullable=True)

    is_active = Column(Boolean, default=True, nullable=False)
    last_used_at = Column(DateTime(timezone=True), nullable=True)
    expires_at = Column(DateTime(timezone=True), nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    # Relationships
    user = relationship("User", back_populates="api_keys")

    def __repr__(self):
        return f"<APIKey {self.name}>"
