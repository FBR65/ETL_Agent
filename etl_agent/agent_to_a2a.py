"""
Agent-to-Agent Communication Server für ETL-Agent
"""

import asyncio
import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
from typing import Dict, Any, Optional

from .etl_agent_core import ETLAgent, ETLRequest, ETLResponse

logger = logging.getLogger(__name__)

app = FastAPI(
    title="ETL Agent A2A Server",
    description="Agent-to-Agent Communication Server für ETL-Operationen",
    version="1.0.0",
)

# Global ETL Agent Instance
etl_agent = ETLAgent()


class A2ARequest(BaseModel):
    """A2A Request Model"""

    action: str
    payload: Dict[str, Any]
    source_agent: Optional[str] = None
    target_agent: str = "etl_agent"


class A2AResponse(BaseModel):
    """A2A Response Model"""

    status: str
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    source_agent: str = "etl_agent"


@app.get("/health")
async def health_check():
    """Health Check Endpoint"""
    return {"status": "healthy", "service": "ETL Agent A2A"}


@app.post("/a2a/execute", response_model=A2AResponse)
async def execute_a2a_request(request: A2ARequest):
    """Führt A2A-Anfrage aus"""
    try:
        logger.info(f"A2A Request: {request.action} von {request.source_agent}")

        if request.action == "generate_etl":
            # ETL-Code generieren
            etl_request = ETLRequest(**request.payload)
            result = await etl_agent.process_etl_request(etl_request)

            return A2AResponse(
                status="success",
                data={
                    "generated_code": result.generated_code,
                    "execution_plan": result.execution_plan,
                    "metadata": result.metadata,
                },
            )

        elif request.action == "list_connections":
            # Verfügbare Datenbankverbindungen auflisten
            connections = etl_agent.db_manager.list_connections()
            return A2AResponse(status="success", data={"connections": connections})

        elif request.action == "test_connection":
            # Datenbankverbindung testen
            conn_name = request.payload.get("connection_name")
            if not conn_name:
                raise HTTPException(400, "connection_name required")

            result = etl_agent.db_manager.test_connection(conn_name)
            return A2AResponse(status="success", data={"test_result": result})

        else:
            raise HTTPException(400, f"Unknown action: {request.action}")

    except Exception as e:
        logger.error(f"A2A Request Fehler: {e}")
        return A2AResponse(
            status="error",
            error=str(e),
        )


@app.get("/a2a/capabilities")
async def get_capabilities():
    """Gibt verfügbare A2A-Capabilities zurück"""
    return {
        "agent_type": "etl_agent",
        "capabilities": [
            "generate_etl",
            "list_connections",
            "test_connection",
        ],
        "supported_databases": [
            "mongodb",
            "postgresql",
            "mysql",
            "mariadb",
            "sqlite",
            "oracle",
            "sqlserver",
        ],
    }


def main():
    """Startet A2A Server"""
    uvicorn.run(
        "etl_agent.agent_to_a2a:app",
        host="0.0.0.0",
        port=8091,
        reload=False,
        access_log=True,
    )


if __name__ == "__main__":
    main()
