from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.core.config import config
from app.core.logging import setup_logging
from app.api.v1.routes import health, query
import logging

# Setup logging
setup_logging(config.logging_mode)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="OptiSQL Frontend API",
    description="FastAPI server for SQL query processing and optimization",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(health.router, prefix="/api/v1")
app.include_router(query.router, prefix="/api/v1")

@app.on_event("startup")
async def startup_event():
    logger.info("Starting OptiSQL Frontend API")
    logger.info(f"Server configuration - Host: {config.host}, Port: {config.port}")
    logger.info(f"Logging mode: {config.logging_mode}")

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Shutting down OptiSQL Frontend API")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host=config.host,
        port=config.port,
        reload=True if config.logging_mode == "debug" else False
    )
