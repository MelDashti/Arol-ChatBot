"""
Admin endpoints for user and system management
"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from typing import List

from app.core.database import get_db
from app.core.security import get_current_admin_user
from app.models.user import User, UserRole
from app.models.conversation import Conversation, Message
from app.schemas.user import UserResponse

router = APIRouter()


@router.get("/users", response_model=List[UserResponse])
async def list_users(
    skip: int = 0,
    limit: int = 100,
    admin_user: User = Depends(get_current_admin_user),
    db: AsyncSession = Depends(get_db)
):
    """List all users (admin only)"""

    result = await db.execute(
        select(User).offset(skip).limit(limit)
    )
    users = result.scalars().all()
    return users


@router.get("/users/{user_id}", response_model=UserResponse)
async def get_user(
    user_id: int,
    admin_user: User = Depends(get_current_admin_user),
    db: AsyncSession = Depends(get_db)
):
    """Get user details (admin only)"""

    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    return user


@router.patch("/users/{user_id}/activate")
async def activate_user(
    user_id: int,
    admin_user: User = Depends(get_current_admin_user),
    db: AsyncSession = Depends(get_db)
):
    """Activate a user account (admin only)"""

    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    user.is_active = True
    await db.commit()
    return {"message": f"User {user.username} activated"}


@router.patch("/users/{user_id}/deactivate")
async def deactivate_user(
    user_id: int,
    admin_user: User = Depends(get_current_admin_user),
    db: AsyncSession = Depends(get_db)
):
    """Deactivate a user account (admin only)"""

    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    if user.role == UserRole.SUPER_ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Cannot deactivate super admin"
        )

    user.is_active = False
    await db.commit()
    return {"message": f"User {user.username} deactivated"}


@router.get("/stats")
async def get_system_stats(
    admin_user: User = Depends(get_current_admin_user),
    db: AsyncSession = Depends(get_db)
):
    """Get system statistics (admin only)"""

    # User stats
    total_users = await db.execute(select(func.count(User.id)))
    active_users = await db.execute(
        select(func.count(User.id)).where(User.is_active == True)
    )

    # Conversation stats
    total_conversations = await db.execute(select(func.count(Conversation.id)))
    total_messages = await db.execute(select(func.count(Message.id)))

    # Average messages per conversation
    avg_messages_result = await db.execute(
        select(func.avg(func.count(Message.id)))
        .select_from(Message)
        .join(Conversation)
        .group_by(Conversation.id)
    )
    avg_messages = avg_messages_result.scalar() or 0

    return {
        "users": {
            "total": total_users.scalar(),
            "active": active_users.scalar()
        },
        "conversations": {
            "total": total_conversations.scalar()
        },
        "messages": {
            "total": total_messages.scalar(),
            "average_per_conversation": round(float(avg_messages), 2)
        }
    }
