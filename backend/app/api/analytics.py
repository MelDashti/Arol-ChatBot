"""
Analytics endpoints for tracking user behavior and system performance
"""

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_
from datetime import datetime, timedelta
from typing import List, Dict, Any

from app.core.database import get_db
from app.core.security import get_current_user
from app.models.user import User
from app.models.conversation import Conversation, Message

router = APIRouter()


@router.get("/my-stats")
async def get_user_stats(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Get current user's statistics"""

    # Total conversations
    total_convs = await db.execute(
        select(func.count(Conversation.id)).where(Conversation.user_id == current_user.id)
    )

    # Total messages
    total_msgs = await db.execute(
        select(func.count(Message.id))
        .join(Conversation)
        .where(Conversation.user_id == current_user.id)
    )

    # Average response time
    avg_time = await db.execute(
        select(func.avg(Message.processing_time_ms))
        .join(Conversation)
        .where(
            and_(
                Conversation.user_id == current_user.id,
                Message.role == "assistant",
                Message.processing_time_ms.isnot(None)
            )
        )
    )

    # Messages in last 7 days
    seven_days_ago = datetime.utcnow() - timedelta(days=7)
    recent_msgs = await db.execute(
        select(func.count(Message.id))
        .join(Conversation)
        .where(
            and_(
                Conversation.user_id == current_user.id,
                Message.created_at >= seven_days_ago
            )
        )
    )

    # Average rating
    avg_rating = await db.execute(
        select(func.avg(Message.rating))
        .join(Conversation)
        .where(
            and_(
                Conversation.user_id == current_user.id,
                Message.rating.isnot(None)
            )
        )
    )

    return {
        "total_conversations": total_convs.scalar() or 0,
        "total_messages": total_msgs.scalar() or 0,
        "messages_last_7_days": recent_msgs.scalar() or 0,
        "average_response_time_ms": round(avg_time.scalar() or 0, 2),
        "average_rating": round(avg_rating.scalar() or 0, 2)
    }


@router.get("/activity")
async def get_activity_timeline(
    days: int = 30,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Get user activity timeline"""

    start_date = datetime.utcnow() - timedelta(days=days)

    # Messages per day
    result = await db.execute(
        select(
            func.date(Message.created_at).label('date'),
            func.count(Message.id).label('count')
        )
        .join(Conversation)
        .where(
            and_(
                Conversation.user_id == current_user.id,
                Message.created_at >= start_date
            )
        )
        .group_by(func.date(Message.created_at))
        .order_by(func.date(Message.created_at))
    )

    activity_data = []
    for row in result:
        activity_data.append({
            "date": row.date.isoformat(),
            "message_count": row.count
        })

    return {"activity": activity_data}


@router.get("/popular-topics")
async def get_popular_topics(
    limit: int = 10,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Get most discussed topics (based on conversation titles)"""

    result = await db.execute(
        select(Conversation.title, func.count(Message.id).label('message_count'))
        .join(Message)
        .where(
            and_(
                Conversation.user_id == current_user.id,
                Conversation.title.isnot(None)
            )
        )
        .group_by(Conversation.title)
        .order_by(func.count(Message.id).desc())
        .limit(limit)
    )

    topics = []
    for row in result:
        topics.append({
            "title": row.title,
            "message_count": row.message_count
        })

    return {"topics": topics}
