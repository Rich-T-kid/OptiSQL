from fastapi import APIRouter
from app.models.schemas import HealthResponse
import logging

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """
    Health check endpoint to verify the service is running.

    Returns:
        HealthResponse: Current health status of the service
    """
    logger.info("Health check requested")
    return HealthResponse(status="healthy", version="0.1.0")
