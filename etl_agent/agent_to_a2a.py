"""
A2A Server für ETL-Agent
Agent-to-Agent Communication Server
"""

import asyncio
import logging
from fastapi import FastAPI
from .etl_agent_core import ETLAgent

logger = logging.getLogger(__name__)

# ETL-Agent erstellen
etl_agent = ETLAgent()

# A2A-App erstellen - Das ist ein FastA2A Objekt, nicht FastAPI
a2a_app = etl_agent.to_a2a()

# Separate FastAPI App für zusätzliche Endpoints
app = FastAPI(
    title="ETL Agent A2A Service",
    description="Agent-to-Agent Communication Server",
    version="1.0.0",
)


@app.get("/health")
async def health_check():
    """Gesundheitscheck für den A2A Service"""
    return {"status": "healthy", "service": "etl-agent-a2a"}


@app.get("/")
async def root():
    """Root-Endpoint"""
    return {"message": "ETL Agent A2A Service", "version": "1.0.0"}


# A2A App in FastAPI mounten
app.mount("/a2a", a2a_app)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8091)
    uvicorn.run(app, host="0.0.0.0", port=8081)
