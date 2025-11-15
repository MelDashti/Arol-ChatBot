"""
Health check and monitoring endpoints
"""

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
import psutil
import torch
from datetime import datetime

from app.core.database import get_db
from app.core.cache import cache_manager
from app.core.ml_model import model_manager
from app.core.websocket_manager import ws_manager

router = APIRouter()


@router.get("/health")
async def health_check():
    """Basic health check"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "arol-chatbot-api"
    }


@router.get("/health/detailed")
async def detailed_health_check(db: AsyncSession = Depends(get_db)):
    """Detailed health check with all dependencies"""

    health_status = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "checks": {}
    }

    # Database check
    try:
        await db.execute(text("SELECT 1"))
        health_status["checks"]["database"] = {"status": "healthy"}
    except Exception as e:
        health_status["checks"]["database"] = {"status": "unhealthy", "error": str(e)}
        health_status["status"] = "unhealthy"

    # Redis check
    try:
        if cache_manager.redis:
            await cache_manager.redis.ping()
            health_status["checks"]["redis"] = {"status": "healthy"}
        else:
            health_status["checks"]["redis"] = {"status": "not_connected"}
    except Exception as e:
        health_status["checks"]["redis"] = {"status": "unhealthy", "error": str(e)}

    # ML Model check
    health_status["checks"]["ml_model"] = {
        "status": "loaded" if model_manager.loaded else "not_loaded"
    }

    # GPU check
    if torch.cuda.is_available():
        health_status["checks"]["gpu"] = {
            "status": "available",
            "device": torch.cuda.get_device_name(0),
            "memory_allocated_gb": round(torch.cuda.memory_allocated() / 1024**3, 2),
            "memory_reserved_gb": round(torch.cuda.memory_reserved() / 1024**3, 2)
        }
    else:
        health_status["checks"]["gpu"] = {"status": "not_available"}

    # WebSocket connections
    health_status["checks"]["websocket"] = {
        "active_connections": ws_manager.get_connection_count()
    }

    return health_status


@router.get("/health/metrics")
async def system_metrics():
    """System resource metrics"""
    cpu_percent = psutil.cpu_percent(interval=1)
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage('/')

    metrics = {
        "cpu": {
            "percent": cpu_percent,
            "count": psutil.cpu_count()
        },
        "memory": {
            "total_gb": round(memory.total / 1024**3, 2),
            "available_gb": round(memory.available / 1024**3, 2),
            "percent": memory.percent
        },
        "disk": {
            "total_gb": round(disk.total / 1024**3, 2),
            "used_gb": round(disk.used / 1024**3, 2),
            "free_gb": round(disk.free / 1024**3, 2),
            "percent": disk.percent
        }
    }

    if torch.cuda.is_available():
        metrics["gpu"] = {
            "memory_allocated_gb": round(torch.cuda.memory_allocated() / 1024**3, 2),
            "memory_reserved_gb": round(torch.cuda.memory_reserved() / 1024**3, 2),
            "memory_total_gb": round(torch.cuda.get_device_properties(0).total_memory / 1024**3, 2)
        }

    return metrics
