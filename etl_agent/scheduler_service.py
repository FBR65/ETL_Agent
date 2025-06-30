"""
ETL Scheduler Service
"""

import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from .scheduler import ETLScheduler
import uvicorn

logger = logging.getLogger(__name__)

scheduler = ETLScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    scheduler.start()
    logger.info("ETL Scheduler gestartet")
    yield
    # Shutdown
    scheduler.stop()
    logger.info("ETL Scheduler gestoppt")


app = FastAPI(
    title="ETL Scheduler Service",
    description="Job Scheduler für ETL-Pipelines",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/")
async def root():
    return {"message": "ETL Scheduler Service", "version": "0.1.0"}


@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "scheduler"}


@app.get("/jobs")
async def list_jobs():
    """Liste alle geplanten Jobs"""
    return {"jobs": scheduler.list_jobs()}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8092)
    uvicorn.run(app, host="0.0.0.0", port=8092)
