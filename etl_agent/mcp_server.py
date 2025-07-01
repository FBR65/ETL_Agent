"""
MCP Server für ETL-Agent - Verbesserte Implementierung
Model Context Protocol Server mit erweiterten Tool-Funktionen
"""

import logging
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Dict, List, Any, Optional

from .database_manager import DatabaseManager
from .etl_agent_core_clean import ETLAgent, ETLRequest

logger = logging.getLogger(__name__)

app = FastAPI(
    title="ETL Agent MCP Server",
    description="Model Context Protocol Server für ETL-Operationen mit erweiterten Tools",
    version="2.0.0",
)

# Database Manager und ETL Agent initialisieren
db_manager = DatabaseManager()
etl_agent = ETLAgent(db_manager=db_manager)


class MCPToolRequest(BaseModel):
    """MCP Tool Request Model"""

    name: str
    arguments: Dict[str, Any]


class MCPToolResponse(BaseModel):
    """MCP Tool Response Model"""

    content: List[Dict[str, Any]]
    error: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


@app.get("/")
async def root():
    """Root Endpoint"""
    return {
        "message": "ETL Agent MCP Server",
        "version": "2.0.0",
        "protocol": "Model Context Protocol",
        "capabilities": "Enhanced ETL Tools",
    }


@app.get("/health")
async def health_check():
    """Health Check Endpoint"""
    return {
        "status": "healthy",
        "services": "mcp_server",
        "version": "2.0.0",
        "tools_available": 7,
    }


@app.get("/tools")
async def list_tools():
    """Liste alle verfügbaren MCP Tools - Erweiterte Version"""
    return {
        "tools": [
            {
                "name": "list_database_connections",
                "description": "Liste alle verfügbaren Datenbankverbindungen mit Details",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "include_details": {
                            "type": "boolean",
                            "description": "Ob detaillierte Informationen zurückgegeben werden sollen",
                            "default": False,
                        }
                    },
                    "required": [],
                },
            },
            {
                "name": "get_database_schema",
                "description": "Hole detailliertes Datenbankschema für eine Verbindung",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "connection_name": {
                            "type": "string",
                            "description": "Name der Datenbankverbindung",
                        },
                        "include_columns": {
                            "type": "boolean",
                            "description": "Ob Spalteninformationen eingeschlossen werden sollen",
                            "default": True,
                        },
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
                        "preview_only": {
                            "type": "boolean",
                            "description": "Nur Vorschau der ersten Zeilen",
                            "default": True,
                        },
                    },
                    "required": ["connection_name", "query_config"],
                },
            },
            {
                "name": "test_connection",
                "description": "Teste eine Datenbankverbindung",
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
                "name": "generate_etl_code",
                "description": "Generiere ETL-Code aus natürlichsprachiger Beschreibung",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "description": {
                            "type": "string",
                            "description": "Natürlichsprachige Beschreibung des ETL-Prozesses",
                        },
                        "source_config": {
                            "type": "object",
                            "description": "Optionale Quell-Konfiguration",
                        },
                        "target_config": {
                            "type": "object",
                            "description": "Optionale Ziel-Konfiguration",
                        },
                    },
                    "required": ["description"],
                },
            },
            {
                "name": "validate_etl_config",
                "description": "Validiere ETL-Konfiguration gegen verfügbare Verbindungen",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "source_connection": {
                            "type": "string",
                            "description": "Name der Quell-Verbindung",
                        },
                        "target_connection": {
                            "type": "string",
                            "description": "Name der Ziel-Verbindung",
                        },
                        "transformation_rules": {
                            "type": "array",
                            "description": "Liste der Transformationsregeln",
                        },
                    },
                    "required": ["source_connection"],
                },
            },
            {
                "name": "get_connection_capabilities",
                "description": "Hole Capabilities und Eigenschaften einer Datenbankverbindung",
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
        ]
    }


@app.post("/mcp/call-tool")
async def call_mcp_tool(request: MCPToolRequest) -> MCPToolResponse:
    """Führe MCP Tool aus - Erweiterte Tool-Ausführung"""
    try:
        tool_name = request.name
        arguments = request.arguments

        logger.info(f"🔧 MCP Tool called: {tool_name} with args: {arguments}")

        if tool_name == "list_database_connections":
            return await list_database_connections_tool(arguments)
        elif tool_name == "get_database_schema":
            return await get_database_schema_tool(arguments)
        elif tool_name == "extract_data":
            return await extract_data_tool(arguments)
        elif tool_name == "test_connection":
            return await test_connection_tool(arguments)
        elif tool_name == "generate_etl_code":
            return await generate_etl_code_tool(arguments)
        elif tool_name == "validate_etl_config":
            return await validate_etl_config_tool(arguments)
        elif tool_name == "get_connection_capabilities":
            return await get_connection_capabilities_tool(arguments)
        else:
            return MCPToolResponse(content=[], error=f"Unknown tool: {tool_name}")

    except Exception as e:
        logger.error(f"❌ MCP Tool error: {e}")
        return MCPToolResponse(content=[], error=str(e))


async def list_database_connections_tool(arguments: Dict[str, Any]) -> MCPToolResponse:
    """Tool: Liste Datenbankverbindungen - Erweitert"""
    try:
        include_details = arguments.get("include_details", False)
        connections = db_manager.list_connections()

        if include_details:
            detailed_connections = {}
            for conn_name in connections:
                try:
                    test_result = db_manager.test_connection(conn_name)
                    conn_config = db_manager.connection_configs.get(conn_name, {})
                    detailed_connections[conn_name] = {
                        "type": conn_config.get("type", "unknown"),
                        "status": test_result.get("status", "unknown"),
                        "message": test_result.get("message", ""),
                    }
                except Exception as e:
                    detailed_connections[conn_name] = {
                        "type": "unknown",
                        "status": "error",
                        "message": str(e),
                    }

            return MCPToolResponse(
                content=[
                    {
                        "type": "text",
                        "text": "Verfügbare Datenbankverbindungen mit Details:",
                    },
                    {
                        "type": "resource",
                        "resource": {
                            "uri": "connections://details",
                            "name": "Database Connections Details",
                            "mimeType": "application/json",
                        },
                    },
                ],
                metadata={
                    "connections": detailed_connections,
                    "total_count": len(connections),
                },
            )
        else:
            return MCPToolResponse(
                content=[
                    {
                        "type": "text",
                        "text": f"Verfügbare Datenbankverbindungen: {', '.join(connections)}",
                    }
                ],
                metadata={"connections": connections, "total_count": len(connections)},
            )
    except Exception as e:
        return MCPToolResponse(content=[], error=str(e))


async def get_database_schema_tool(arguments: Dict[str, Any]) -> MCPToolResponse:
    """Tool: Hole Datenbankschema - Erweitert"""
    try:
        connection_name = arguments.get("connection_name")
        include_columns = arguments.get("include_columns", True)

        if not connection_name:
            return MCPToolResponse(content=[], error="connection_name required")

        schema = db_manager.get_database_schema(connection_name)

        schema_text = f"Schema für {connection_name}:"
        if isinstance(schema, dict):
            if "tables" in schema:
                schema_text += f"\nTabellen: {', '.join(schema['tables'])}"
            if include_columns and "columns" in schema:
                schema_text += (
                    f"\nSpalten-Details verfügbar: {len(schema['columns'])} Tabellen"
                )

        return MCPToolResponse(
            content=[
                {"type": "text", "text": schema_text},
                {
                    "type": "resource",
                    "resource": {
                        "uri": f"schema://{connection_name}",
                        "name": f"Schema for {connection_name}",
                        "mimeType": "application/json",
                    },
                },
            ],
            metadata={"connection_name": connection_name, "schema": schema},
        )
    except Exception as e:
        return MCPToolResponse(content=[], error=str(e))


async def extract_data_tool(arguments: Dict[str, Any]) -> MCPToolResponse:
    """Tool: Extrahiere Daten - Erweitert"""
    try:
        connection_name = arguments.get("connection_name")
        query_config = arguments.get("query_config")
        preview_only = arguments.get("preview_only", True)

        if not connection_name or not query_config:
            return MCPToolResponse(
                content=[], error="connection_name and query_config required"
            )

        df = db_manager.extract_data(connection_name, query_config)

        # Preview-Modus: Nur erste 10 Zeilen
        if preview_only and len(df) > 10:
            preview_df = df.head(10)
            result_text = f"Daten extrahiert (Vorschau): {len(preview_df)} von {len(df)} Zeilen, Spalten: {list(df.columns)}"
        else:
            preview_df = df
            result_text = (
                f"Daten extrahiert: {len(df)} Zeilen, Spalten: {list(df.columns)}"
            )

        return MCPToolResponse(
            content=[
                {
                    "type": "text",
                    "text": result_text,
                },
                {
                    "type": "resource",
                    "resource": {
                        "uri": f"data://{connection_name}/extracted",
                        "name": f"Extracted data from {connection_name}",
                        "mimeType": "text/csv",
                    },
                },
            ],
            metadata={
                "total_rows": len(df),
                "preview_rows": len(preview_df),
                "columns": list(df.columns),
                "data_preview": preview_df.to_dict("records")
                if len(preview_df) > 0
                else [],
            },
        )
    except Exception as e:
        return MCPToolResponse(content=[], error=str(e))


async def test_connection_tool(arguments: Dict[str, Any]) -> MCPToolResponse:
    """Tool: Teste Datenbankverbindung"""
    try:
        connection_name = arguments.get("connection_name")
        if not connection_name:
            return MCPToolResponse(content=[], error="connection_name required")

        result = db_manager.test_connection(connection_name)

        status_emoji = "✅" if result.get("status") == "success" else "❌"

        return MCPToolResponse(
            content=[
                {
                    "type": "text",
                    "text": f"{status_emoji} Verbindungstest für {connection_name}: {result.get('message', 'No message')}",
                }
            ],
            metadata={"connection_name": connection_name, "test_result": result},
        )
    except Exception as e:
        return MCPToolResponse(content=[], error=str(e))


async def generate_etl_code_tool(arguments: Dict[str, Any]) -> MCPToolResponse:
    """Tool: Generiere ETL-Code mit PydanticAI"""
    try:
        description = arguments.get("description")
        if not description:
            return MCPToolResponse(content=[], error="description required")

        source_config = arguments.get("source_config")
        target_config = arguments.get("target_config")

        etl_request = ETLRequest(
            description=description,
            source_config=source_config,
            target_config=target_config,
        )

        result = await etl_agent.process_etl_request(etl_request)

        if result.status == "success":
            return MCPToolResponse(
                content=[
                    {
                        "type": "text",
                        "text": f"✅ ETL-Code erfolgreich generiert für: {description}",
                    },
                    {
                        "type": "resource",
                        "resource": {
                            "uri": "code://etl/generated",
                            "name": "Generated ETL Code",
                            "mimeType": "text/x-python",
                        },
                    },
                ],
                metadata={
                    "generated_code": result.generated_code,
                    "execution_plan": result.execution_plan,
                    "model_info": result.metadata,
                },
            )
        else:
            return MCPToolResponse(
                content=[
                    {
                        "type": "text",
                        "text": f"❌ ETL-Code Generierung fehlgeschlagen: {result.error_message}",
                    }
                ],
                error=result.error_message,
            )

    except Exception as e:
        return MCPToolResponse(content=[], error=str(e))


async def validate_etl_config_tool(arguments: Dict[str, Any]) -> MCPToolResponse:
    """Tool: Validiere ETL-Konfiguration"""
    try:
        source_connection = arguments.get("source_connection")
        target_connection = arguments.get("target_connection")
        transformation_rules = arguments.get("transformation_rules", [])

        if not source_connection:
            return MCPToolResponse(content=[], error="source_connection required")

        validation_results = {
            "source_connection_valid": False,
            "target_connection_valid": False,
            "transformations_count": len(transformation_rules),
            "issues": [],
        }

        # Quell-Verbindung validieren
        try:
            source_test = db_manager.test_connection(source_connection)
            validation_results["source_connection_valid"] = (
                source_test.get("status") == "success"
            )
            if not validation_results["source_connection_valid"]:
                validation_results["issues"].append(
                    f"Source connection failed: {source_test.get('message', 'Unknown error')}"
                )
        except Exception as e:
            validation_results["issues"].append(f"Source connection error: {str(e)}")

        # Ziel-Verbindung validieren (falls angegeben)
        if target_connection:
            try:
                target_test = db_manager.test_connection(target_connection)
                validation_results["target_connection_valid"] = (
                    target_test.get("status") == "success"
                )
                if not validation_results["target_connection_valid"]:
                    validation_results["issues"].append(
                        f"Target connection failed: {target_test.get('message', 'Unknown error')}"
                    )
            except Exception as e:
                validation_results["issues"].append(
                    f"Target connection error: {str(e)}"
                )

        # Gesamt-Validierung
        overall_valid = validation_results["source_connection_valid"] and (
            not target_connection or validation_results["target_connection_valid"]
        )

        status_emoji = "✅" if overall_valid else "❌"
        issues_text = (
            f", Probleme: {'; '.join(validation_results['issues'])}"
            if validation_results["issues"]
            else ""
        )

        return MCPToolResponse(
            content=[
                {
                    "type": "text",
                    "text": f"{status_emoji} ETL-Konfiguration Validierung: {'Erfolgreich' if overall_valid else 'Fehlgeschlagen'}{issues_text}",
                }
            ],
            metadata=validation_results,
        )
    except Exception as e:
        return MCPToolResponse(content=[], error=str(e))


async def get_connection_capabilities_tool(
    arguments: Dict[str, Any],
) -> MCPToolResponse:
    """Tool: Hole Connection Capabilities"""
    try:
        connection_name = arguments.get("connection_name")
        if not connection_name:
            return MCPToolResponse(content=[], error="connection_name required")

        # Connection-Details sammeln
        conn_config = db_manager.connection_configs.get(connection_name, {})
        conn_type = conn_config.get("type", "unknown")

        capabilities = {
            "connection_name": connection_name,
            "database_type": conn_type,
            "read_operations": True,
            "write_operations": True,
            "schema_introspection": True,
            "supported_formats": [],
        }

        # Typ-spezifische Capabilities
        if conn_type == "mongodb":
            capabilities["supported_formats"] = ["json", "bson"]
            capabilities["aggregation_pipeline"] = True
            capabilities["full_text_search"] = True
        elif conn_type in ["postgresql", "mysql", "mariadb", "oracle", "sqlserver"]:
            capabilities["supported_formats"] = ["sql"]
            capabilities["joins"] = True
            capabilities["stored_procedures"] = True
            capabilities["indexes"] = True
        elif conn_type == "sqlite":
            capabilities["supported_formats"] = ["sql"]
            capabilities["file_based"] = True
            capabilities["lightweight"] = True

        # Test ob Verbindung aktiv ist
        try:
            test_result = db_manager.test_connection(connection_name)
            capabilities["connection_active"] = test_result.get("status") == "success"
            capabilities["last_test_message"] = test_result.get("message", "")
        except Exception:
            capabilities["connection_active"] = False

        return MCPToolResponse(
            content=[
                {
                    "type": "text",
                    "text": f"Capabilities für {connection_name} ({conn_type}): Aktiv={capabilities['connection_active']}, Read/Write verfügbar",
                }
            ],
            metadata=capabilities,
        )
    except Exception as e:
        return MCPToolResponse(content=[], error=str(e))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8090)
