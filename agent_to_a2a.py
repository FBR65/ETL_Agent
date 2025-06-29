"""
A2A Server für ETL-Agent
"""

import asyncio
import logging
from fastapi import FastAPI
from etl_agent_core import ETLAgent

logger = logging.getLogger(__name__)

# ETL-Agent erstellen
etl_agent = ETLAgent()

# A2A-App erstellen
app = etl_agent.to_a2a()


@app.get("/health")
async def health_check():
    """Gesundheitscheck für den Service"""
    return {"status": "healthy", "service": "etl-agent"}


@app.get("/")
async def root():
    """Root-Endpoint"""
    return {"message": "ETL Agent A2A Service", "version": "1.0.0"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
