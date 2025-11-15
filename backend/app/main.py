"""
FastAPI main application entry point
Production-ready AI chatbot with authentication, caching, and monitoring
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from prometheus_fastapi_instrumentator import Instrumentator
import logging
from typing import AsyncGenerator

from app.api import chat, health, auth, admin, analytics
from app.core.config import settings
from app.core.ml_model import MLModelManager
from app.core.cache import cache_manager
from app.core.database import engine, Base
from app.core.websocket_manager import ConnectionManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global instances
model_manager = MLModelManager()
ws_manager = ConnectionManager()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    """Application lifespan handler for startup and shutdown"""
    # Startup
    logger.info("Starting Arol Chatbot API...")

    # Initialize database
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Load ML models
    logger.info("Loading ML models...")
    await model_manager.load_models()

    # Initialize cache
    await cache_manager.connect()

    logger.info("Application started successfully!")

    yield

    # Shutdown
    logger.info("Shutting down...")
    await cache_manager.disconnect()
    await model_manager.cleanup()
    logger.info("Shutdown complete")


# Create FastAPI app
app = FastAPI(
    title="Arol AI Chatbot API",
    description="Production-grade AI chatbot with RAG, authentication, and real-time streaming",
    version="2.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
    lifespan=lifespan
)

# Add middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=1000)

# Prometheus metrics
Instrumentator().instrument(app).expose(app, endpoint="/metrics")

# Include routers
app.include_router(health.router, prefix="/api", tags=["Health"])
app.include_router(auth.router, prefix="/api/auth", tags=["Authentication"])
app.include_router(chat.router, prefix="/api/chat", tags=["Chat"])
app.include_router(admin.router, prefix="/api/admin", tags=["Admin"])
app.include_router(analytics.router, prefix="/api/analytics", tags=["Analytics"])


# WebSocket endpoint for real-time chat
@app.websocket("/ws/chat")
async def websocket_chat(websocket: WebSocket):
    """WebSocket endpoint for streaming chat responses"""
    await ws_manager.connect(websocket)
    try:
        while True:
            # Receive message
            data = await websocket.receive_json()
            message = data.get("message", "")
            conversation_id = data.get("conversation_id")

            # Stream response word by word
            async for chunk in model_manager.stream_response(message, conversation_id):
                await websocket.send_json({
                    "type": "chunk",
                    "content": chunk
                })

            # Send completion signal
            await websocket.send_json({
                "type": "complete",
                "content": ""
            })

    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
        logger.info("WebSocket disconnected")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        await websocket.send_json({
            "type": "error",
            "content": str(e)
        })


# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    logger.error(f"Global exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "detail": "Internal server error",
            "error": str(exc) if settings.DEBUG else "An error occurred"
        }
    )


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "Arol AI Chatbot API",
        "version": "2.0.0",
        "docs": "/api/docs",
        "health": "/api/health"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.DEBUG,
        log_level="info"
    )
