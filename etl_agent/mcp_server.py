"""
MCP Server für ETL-Agent
Model Context Protocol Server
"""

import logging
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Dict, List, Any, Optional
import os

from .database_manager import DatabaseManager
from .connectors.mongodb_connector import MongoDBConnector

logger = logging.getLogger(__name__)

app = FastAPI(
    title="ETL Agent MCP Server",
    description="Model Context Protocol Server für ETL-Operationen",
    version="0.1.0",
)

# Database Manager initialisieren
db_manager = DatabaseManager()


class MCPToolRequest(BaseModel):
    name: str
    arguments: Dict[str, Any]


class MCPToolResponse(BaseModel):
    content: List[Dict[str, Any]]
    error: Optional[str] = None


@app.get("/")
async def root():
    return {"message": "ETL Agent MCP Server", "version": "0.1.0"}


@app.get("/health")
async def health_check():
    return {"status": "healthy", "services": "mcp_server"}


@app.get("/tools")
async def list_tools():
    """Liste alle verfügbaren MCP Tools"""
    return {
        "tools": [
            {
                "name": "list_database_connections",
                "description": "Liste alle verfügbaren Datenbankverbindungen",
                "input_schema": {"type": "object", "properties": {}, "required": []},
            },
            {
                "name": "get_database_schema",
                "description": "Hole Datenbankschema für eine Verbindung",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "connection_name": {
                            "type": "string",
                            "description": "Name der Datenbankverbindung",
                        }
                    },
                    "required": ["connection_name"],
                },
            },
            {
                "name": "extract_data",
                "description": "Extrahiere Daten aus einer Datenbank",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "connection_name": {
                            "type": "string",
                            "description": "Name der Datenbankverbindung",
                        },
                        "query_config": {
                            "type": "object",
                            "description": "Abfrage-Konfiguration",
                        },
                    },
                    "required": ["connection_name", "query_config"],
                },
            },
        ]
    }


@app.post("/mcp/call-tool")
async def call_mcp_tool(request: MCPToolRequest) -> MCPToolResponse:
    """Führe MCP Tool aus"""
    try:
        tool_name = request.name
        arguments = request.arguments

        logger.info(f"🔧 MCP Tool called: {tool_name} with args: {arguments}")

        if tool_name == "list_database_connections":
            return await list_database_connections_tool()
        elif tool_name == "get_database_schema":
            return await get_database_schema_tool(arguments)
        elif tool_name == "extract_data":
            return await extract_data_tool(arguments)
        else:
            return MCPToolResponse(content=[], error=f"Unknown tool: {tool_name}")

    except Exception as e:
        logger.error(f"❌ MCP Tool error: {e}")
        return MCPToolResponse(content=[], error=str(e))


async def list_database_connections_tool() -> MCPToolResponse:
    """Tool: Liste Datenbankverbindungen"""
    try:
        connections = db_manager.list_connections()
        return MCPToolResponse(
            content=[
                {
                    "type": "text",
                    "text": f"Verfügbare Datenbankverbindungen: {connections}",
                }
            ]
        )
    except Exception as e:
        return MCPToolResponse(content=[], error=str(e))


async def get_database_schema_tool(arguments: Dict[str, Any]) -> MCPToolResponse:
    """Tool: Hole Datenbankschema"""
    try:
        connection_name = arguments.get("connection_name")
        if not connection_name:
            return MCPToolResponse(content=[], error="connection_name required")

        schema = db_manager.get_database_schema(connection_name)
        return MCPToolResponse(
            content=[
                {"type": "text", "text": f"Schema für {connection_name}: {schema}"}
            ]
        )
    except Exception as e:
        return MCPToolResponse(content=[], error=str(e))


async def extract_data_tool(arguments: Dict[str, Any]) -> MCPToolResponse:
    """Tool: Extrahiere Daten"""
    try:
        connection_name = arguments.get("connection_name")
        query_config = arguments.get("query_config")

        if not connection_name or not query_config:
            return MCPToolResponse(
                content=[], error="connection_name and query_config required"
            )

        df = db_manager.extract_data(connection_name, query_config)

        return MCPToolResponse(
            content=[
                {
                    "type": "text",
                    "text": f"Daten extrahiert: {len(df)} Zeilen, Spalten: {list(df.columns)}",
                }
            ]
        )
    except Exception as e:
        return MCPToolResponse(content=[], error=str(e))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
