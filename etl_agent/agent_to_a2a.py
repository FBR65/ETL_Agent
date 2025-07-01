"""
Agent-to-Agent Communication Server für ETL-Agent
Verbesserte A2A-Implementierung mit PydanticAI-Integration
"""

import asyncio
import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
from typing import Dict, Any, Optional

from .etl_agent_core_clean import ETLAgent, ETLRequest

logger = logging.getLogger(__name__)

app = FastAPI(
    title="ETL Agent A2A Server",
    description="Agent-to-Agent Communication Server für ETL-Operationen mit PydanticAI",
    version="2.0.0",
)

# Global ETL Agent Instance mit verbesserter A2A-Integration
etl_agent = ETLAgent()


class A2ARequest(BaseModel):
    """A2A Request Model - Erweitert für bessere Agent-Kommunikation"""

    action: str
    payload: Dict[str, Any]
    source_agent: Optional[str] = None
    target_agent: str = "etl_agent"
    priority: Optional[int] = 1
    correlation_id: Optional[str] = None


class A2AResponse(BaseModel):
    """A2A Response Model - Erweitert für bessere Fehlerbehandlung"""

    status: str
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    source_agent: str = "etl_agent"
    correlation_id: Optional[str] = None
    processing_time_ms: Optional[float] = None


@app.get("/health")
async def health_check():
    """Health Check Endpoint"""
    return {
        "status": "healthy",
        "service": "ETL Agent A2A",
        "version": "2.0.0",
        "pydantic_ai_enabled": True,
    }


@app.post("/a2a/execute", response_model=A2AResponse)
async def execute_a2a_request(request: A2ARequest):
    """
    Führt A2A-Anfrage aus - Verbesserte Implementierung
    Unterstützt alle ETL-Agent Capabilities über PydanticAI
    """
    start_time = asyncio.get_event_loop().time()

    try:
        logger.info(
            f"A2A Request: {request.action} von {request.source_agent} (ID: {request.correlation_id})"
        )

        if request.action == "generate_etl":
            # ETL-Code generieren mit PydanticAI
            etl_request = ETLRequest(**request.payload)
            result = await etl_agent.process_etl_request(etl_request)

            processing_time = (asyncio.get_event_loop().time() - start_time) * 1000

            return A2AResponse(
                status="success" if result.status == "success" else "error",
                data={
                    "generated_code": result.generated_code,
                    "execution_plan": result.execution_plan,
                    "metadata": result.metadata,
                    "schema_info": result.schema_info,
                }
                if result.status == "success"
                else None,
                error=result.error_message if result.status == "error" else None,
                correlation_id=request.correlation_id,
                processing_time_ms=processing_time,
            )

        elif request.action == "list_connections":
            # Verfügbare Datenbankverbindungen auflisten
            connections = etl_agent.db_manager.list_connections()
            connection_details = {}

            # Detaillierte Informationen für jeden Connection sammeln
            for conn_name in connections:
                try:
                    test_result = etl_agent.db_manager.test_connection(conn_name)
                    conn_type = etl_agent.db_manager.connection_configs.get(
                        conn_name, {}
                    ).get("type", "unknown")
                    connection_details[conn_name] = {
                        "type": conn_type,
                        "status": test_result.get("status", "unknown"),
                        "message": test_result.get("message", ""),
                    }
                except Exception as e:
                    connection_details[conn_name] = {
                        "type": "unknown",
                        "status": "error",
                        "message": str(e),
                    }

            processing_time = (asyncio.get_event_loop().time() - start_time) * 1000

            return A2AResponse(
                status="success",
                data={
                    "connections": connections,
                    "connection_details": connection_details,
                    "total_connections": len(connections),
                },
                correlation_id=request.correlation_id,
                processing_time_ms=processing_time,
            )

        elif request.action == "test_connection":
            # Datenbankverbindung testen
            conn_name = request.payload.get("connection_name")
            if not conn_name:
                raise HTTPException(400, "connection_name required")

            result = etl_agent.db_manager.test_connection(conn_name)
            processing_time = (asyncio.get_event_loop().time() - start_time) * 1000

            return A2AResponse(
                status="success",
                data={"test_result": result},
                correlation_id=request.correlation_id,
                processing_time_ms=processing_time,
            )

        elif request.action == "get_schema":
            # Schema-Informationen abrufen
            conn_name = request.payload.get("connection_name")
            if not conn_name:
                raise HTTPException(400, "connection_name required")

            schema = etl_agent.db_manager.get_database_schema(conn_name)
            processing_time = (asyncio.get_event_loop().time() - start_time) * 1000

            return A2AResponse(
                status="success",
                data={"connection_name": conn_name, "schema": schema},
                correlation_id=request.correlation_id,
                processing_time_ms=processing_time,
            )

        elif request.action == "extract_data":
            # Daten extrahieren
            conn_name = request.payload.get("connection_name")
            query_config = request.payload.get("query_config")

            if not conn_name or not query_config:
                raise HTTPException(400, "connection_name and query_config required")

            df = etl_agent.db_manager.extract_data(conn_name, query_config)
            processing_time = (asyncio.get_event_loop().time() - start_time) * 1000

            return A2AResponse(
                status="success",
                data={
                    "rows_extracted": len(df),
                    "columns": list(df.columns),
                    "data_preview": df.head(5).to_dict("records")
                    if len(df) > 0
                    else [],
                    "query_config": query_config,
                },
                correlation_id=request.correlation_id,
                processing_time_ms=processing_time,
            )

        elif request.action == "agent_capabilities":
            # Agent-Capabilities zurückgeben
            processing_time = (asyncio.get_event_loop().time() - start_time) * 1000

            return A2AResponse(
                status="success",
                data={
                    "agent_type": "etl_agent",
                    "version": "2.0.0",
                    "pydantic_ai_enabled": True,
                    "capabilities": [
                        "generate_etl",
                        "list_connections",
                        "test_connection",
                        "get_schema",
                        "extract_data",
                        "agent_capabilities",
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
                    "ai_model": etl_agent.llm_model_name,
                    "ai_endpoint": etl_agent.llm_endpoint,
                },
                correlation_id=request.correlation_id,
                processing_time_ms=processing_time,
            )

        else:
            raise HTTPException(400, f"Unknown action: {request.action}")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"A2A Request Fehler: {e}")
        processing_time = (asyncio.get_event_loop().time() - start_time) * 1000

        return A2AResponse(
            status="error",
            error=str(e),
            correlation_id=request.correlation_id,
            processing_time_ms=processing_time,
        )


@app.get("/a2a/capabilities")
async def get_capabilities():
    """
    Gibt verfügbare A2A-Capabilities zurück
    Erweitert für bessere Agent-Discovery
    """
    return {
        "agent_type": "etl_agent",
        "version": "2.0.0",
        "pydantic_ai_enabled": True,
        "capabilities": [
            {
                "action": "generate_etl",
                "description": "Generiert ETL-Code aus natürlichsprachiger Beschreibung",
                "required_payload": ["description"],
                "optional_payload": [
                    "source_config",
                    "target_config",
                    "transformation_rules",
                ],
            },
            {
                "action": "list_connections",
                "description": "Listet alle verfügbaren Datenbankverbindungen",
                "required_payload": [],
                "optional_payload": [],
            },
            {
                "action": "test_connection",
                "description": "Testet eine spezifische Datenbankverbindung",
                "required_payload": ["connection_name"],
                "optional_payload": [],
            },
            {
                "action": "get_schema",
                "description": "Holt Schema-Informationen für eine Verbindung",
                "required_payload": ["connection_name"],
                "optional_payload": [],
            },
            {
                "action": "extract_data",
                "description": "Extrahiert Daten aus einer Datenbank",
                "required_payload": ["connection_name", "query_config"],
                "optional_payload": [],
            },
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
        "ai_integration": {
            "provider": "OpenAI-compatible",
            "model": etl_agent.llm_model_name,
            "endpoint": etl_agent.llm_endpoint,
            "framework": "PydanticAI",
        },
    }


@app.get("/a2a/status")
async def get_agent_status():
    """Gibt detaillierten Agent-Status zurück"""
    try:
        connections = etl_agent.db_manager.list_connections()
        active_connections = 0

        for conn_name in connections:
            try:
                result = etl_agent.db_manager.test_connection(conn_name)
                if result.get("status") == "success":
                    active_connections += 1
            except Exception:
                pass

        return {
            "agent_status": "active",
            "ai_model_status": "ready",
            "database_connections": {
                "total": len(connections),
                "active": active_connections,
                "inactive": len(connections) - active_connections,
            },
            "capabilities_count": 5,
            "version": "2.0.0",
        }
    except Exception as e:
        return {"agent_status": "error", "error": str(e)}


def main():
    """Startet A2A Server"""
    uvicorn.run(
        "etl_agent.agent_to_a2a_clean:app",
        host="0.0.0.0",
        port=8091,
        reload=False,
        access_log=True,
    )


if __name__ == "__main__":
    main()
