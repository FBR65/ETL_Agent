"""
ETL Agent Core Module mit PydanticAI und MCP/A2A Integration
Verbesserte Implementierung entsprechend dem Gesamtkonzept
"""

import os
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from pydantic import BaseModel, Field
from pydantic_ai import Agent
from pydantic_ai.models.openai import OpenAIModel
from pydantic_ai.providers.openai import OpenAIProvider

from .database_manager import DatabaseManager
from .utils.logger import ETLDesignerLogger

logger = logging.getLogger(__name__)
etl_core_logger = ETLDesignerLogger("etl_core")


class ETLRequest(BaseModel):
    """ETL-Anfrage Struktur"""

    description: str = Field(
        ..., description="Natürlichsprachige Beschreibung des ETL-Prozesses"
    )
    source_config: Optional[Dict[str, Any]] = Field(
        None, description="Quell-Datenbank Konfiguration"
    )
    target_config: Optional[Dict[str, Any]] = Field(
        None, description="Ziel-Datenbank Konfiguration"
    )
    transformation_rules: Optional[List[str]] = Field(
        None, description="Transformationsregeln"
    )
    metadata: Optional[Dict[str, Any]] = Field(
        None, description="Zusätzliche Metadaten"
    )


class ETLResponse(BaseModel):
    """ETL-Antwort Struktur"""

    status: str = Field(..., description="Status der ETL-Operation")
    generated_code: Optional[str] = Field(
        None, description="Generierter Python ETL-Code"
    )
    execution_plan: List[str] = Field(
        default_factory=list, description="Ausführungsplan"
    )
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Metadaten")
    error_message: Optional[str] = Field(None, description="Fehlermeldung")
    schema_info: Optional[Dict[str, Any]] = Field(
        None, description="Schema-Informationen"
    )


class ETLCodeGenerationContext(BaseModel):
    """Kontext für ETL-Code-Generierung"""

    available_connections: List[str] = Field(default_factory=list)
    source_schema: Optional[Dict[str, Any]] = None
    target_schema: Optional[Dict[str, Any]] = None
    transformation_hints: List[str] = Field(default_factory=list)
    performance_requirements: Optional[Dict[str, Any]] = None


class ETLAgent:
    """
    Hauptklasse für den ETL-Agent mit PydanticAI und Multi-Database Support

    Implementiert das Gesamtkonzept:
    - PydanticAI für strukturierte AI-Interaktion
    - A2A-kompatible Agent-Architektur
    - MCP-Integration für Context Management
    - Multi-Database Support
    """

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """Initialisiert den ETL-Agent"""
        self.db_manager = db_manager or DatabaseManager()
        self.setup_llm()
        self.setup_agent()

    def setup_llm(self):
        """LLM-Konfiguration für OpenAI-kompatible Endpunkte"""
        self.llm_endpoint = os.getenv("BASE_URL", "http://localhost:11434/v1")
        self.llm_api_key = os.getenv("API_KEY", "ollama")
        self.llm_model_name = os.getenv("MODEL_NAME", "granite-code:8b")

        logger.info(f"Initialisiere LLM: {self.llm_model_name} @ {self.llm_endpoint}")

        self.provider = OpenAIProvider(
            base_url=self.llm_endpoint, api_key=self.llm_api_key
        )
        self.model = OpenAIModel(provider=self.provider, model_name=self.llm_model_name)

    def setup_agent(self):
        """PydanticAI Agent konfigurieren mit verbessertem System-Prompt"""
        system_prompt = """Du bist ein Python-Code-Generator für eigenständige ETL-Skripte.

🚨 ABSOLUT VERBOTEN:
- NIEMALS "from etl_agent" importieren
- NIEMALS "import etl_agent" 
- NIEMALS "DatabaseManager" verwenden
- NIEMALS externe Abhängigkeiten
- NIEMALS Markdown ```python``` Blöcke

✅ ZWINGEND ERFORDERLICH:
- NUR SimpleDBManager verwenden (im Code definiert)
- NUR Standard-Libraries: pandas, pymongo, sqlalchemy, json, os, logging
- IMMER vollständigen ausführbaren Code generieren
- IMMER etl_pipeline() Funktionsaufruf am Ende

🎯 EXAKTE TEMPLATE-STRUKTUR (NICHT ABWEICHEN!):

import pandas as pd
import logging
import json
import os
from typing import Dict, List, Any
import pymongo
import sqlalchemy as sa

logger = logging.getLogger(__name__)

class SimpleDBManager:
    def __init__(self, config_file: str = "db_connections.json"):
        self.connection_configs = {}
        self.config_file = config_file
        self._load_connections()
    
    def _load_connections(self):
        if os.path.exists(self.config_file):
            with open(self.config_file, "r", encoding="utf-8") as f:
                self.connection_configs = json.load(f)
    
    def extract_data(self, connection_name: str, query_config: Dict[str, Any]) -> pd.DataFrame:
        config = self.connection_configs[connection_name]
        if config["type"] == "mysql":
            engine = sa.create_engine(config["connection_string"])
            return pd.read_sql(query_config["query"], engine)
        elif config["type"] == "mongodb":
            client = pymongo.MongoClient(config["connection_string"])
            db_name = config["connection_string"].split("/")[-1] or "my_test_db"
            db = client[db_name]
            collection = db[query_config["collection"]]
            data = list(collection.find(query_config.get("query", {})).limit(query_config.get("limit", 1000)))
            client.close()
            return pd.DataFrame(data)
        else:
            raise ValueError(f"Nicht unterstützter DB-Typ: {config['type']}")
    
    def load_data(self, connection_name: str, df: pd.DataFrame, target_config: Dict[str, Any]):
        config = self.connection_configs[connection_name]
        if config["type"] == "mongodb":
            client = pymongo.MongoClient(config["connection_string"])
            db_name = config["connection_string"].split("/")[-1] or "my_test_db"
            db = client[db_name]
            collection = db[target_config["collection"]]
            records = df.to_dict("records")
            if target_config.get("operation") == "replace":
                collection.delete_many({})
            collection.insert_many(records)
            client.close()
        else:
            engine = sa.create_engine(config["connection_string"])
            df.to_sql(target_config["table"], engine, if_exists=target_config.get("if_exists", "replace"), index=False)

def etl_pipeline():
    try:
        logger.info("Starte ETL-Pipeline...")
        db_manager = SimpleDBManager()
        
        # EXTRACT
        logger.info("Extrahiere Daten aus Quelle...")
        extract_config = {"query": "SELECT * FROM users_test"}
        df = db_manager.extract_data("Jup", extract_config)
        logger.info(f"Extrahiert: {len(df)} Datensätze")
        
        # TRANSFORM
        logger.info("Transformiere Daten...")
        df['age'] = df['age'] + 2
        logger.info("Transformation abgeschlossen")
        
        # LOAD
        logger.info("Lade Daten in Ziel...")
        load_config = {"collection": "NEUTEST", "operation": "replace"}
        db_manager.load_data("mong4", df, load_config)
        logger.info("Daten erfolgreich geladen")
        
        logger.info("ETL-Pipeline erfolgreich abgeschlossen")
        return df
        
    except Exception as e:
        logger.error(f"ETL-Pipeline Fehler: {e}")
        raise

if __name__ == "__main__":
    result = etl_pipeline()
    print(f"ETL abgeschlossen. Verarbeitete Datensätze: {len(result)}")

🚨 KRITISCHE REGELN:
1. VERWENDE EXAKT diese Struktur
2. ÄNDERE NUR: Tabellennamen, Transformationen, Connection-Namen
3. BEHALTE ALLES ANDERE: Imports, SimpleDBManager, etl_pipeline(), if __name__
4. NIEMALS etl_agent importieren
5. IMMER vollständigen ausführbaren Code generieren

ANTWORTE NUR MIT VOLLSTÄNDIGEM PYTHON-CODE! KEINE ERKLÄRUNGEN! KEINE MARKDOWN!"""

        self.agent = Agent(
            model=self.model,
            result_type=str,
            retries=5,  # Erhöhe Retries für bessere Qualität
            system_prompt=system_prompt,
        )

    def to_a2a(self):
        """
        Erstellt A2A-kompatible App aus dem Agent
        Ermöglicht Agent-to-Agent Communication
        """
        return self.agent.to_a2a()

    async def process_etl_request(self, request: ETLRequest) -> ETLResponse:
        """
        Verarbeitet eine ETL-Anfrage mit Multi-DB Support und verbesserter Context-Analyse
        """
        try:
            # Enhanced Logging: Start
            etl_core_logger.log_user_action(
                "etl_request_processing_started",
                {
                    "description": request.description,
                    "source_config": request.source_config,
                    "target_config": request.target_config,
                    "transformation_rules": request.transformation_rules,
                },
            )

            logger.info(f"Verarbeite ETL-Anfrage: {request.description}")

            # 1. Kontext analysieren und sammeln
            context = await self._build_comprehensive_context(request)

            # 2. Schema-Informationen sammeln
            schema_info = await self._gather_schema_information(request, context)

            # 3. Prompt für Code-Generierung erstellen
            prompt = self._build_enhanced_generation_prompt(
                request, context, schema_info
            )

            logger.info(f"FULL PROMPT LENGTH: {len(prompt)} characters")
            logger.info(f"FULL PROMPT:\n{'-' * 80}\n{prompt}\n{'-' * 80}")

            # 4. Code mit PydanticAI generieren
            try:
                logger.info("=== AI-AGENT DEBUG START ===")
                logger.info(f"REQUEST: {request.description}")
                logger.info(f"VERFÜGBARE VERBINDUNGEN: {context.available_connections}")
                logger.info(f"TRANSFORMATION HINTS: {context.transformation_hints}")
                logger.info(
                    f"SCHEMA INFO: {len(schema_info.get('available_connections_details', {}))} Verbindungen mit Schema"
                )

                # Prompt loggen (gekürzt)
                prompt_preview = prompt[:500] + "..." if len(prompt) > 500 else prompt
                logger.info(f"PROMPT PREVIEW: {prompt_preview}")

                logger.info("Starte AI-Agent für Code-Generierung...")

                # Enhanced Logging: AI Interaction Start
                etl_core_logger.log_user_action(
                    "ai_code_generation_started",
                    {
                        "prompt_length": len(prompt),
                        "available_connections": len(context.available_connections),
                        "schema_connections": len(
                            schema_info.get("available_connections_details", {})
                        ),
                    },
                )

                import time

                ai_start_time = time.time()

                raw_code = await self.agent.run(prompt)

                ai_duration = time.time() - ai_start_time

                logger.info(f"RAW AI RESPONSE TYPE: {type(raw_code)}")
                logger.info(f"RAW AI RESPONSE LENGTH: {len(str(raw_code))}")
                logger.info(
                    f"RAW AI RESPONSE FULL:\n{'-' * 50}\n{raw_code}\n{'-' * 50}"
                )
                logger.info("=== AI-AGENT DEBUG END ===")

                # AgentRunResult richtig verarbeiten - DIREKTES OUTPUT
                if hasattr(raw_code, "output"):
                    # PydanticAI AgentRunResult - extrahiere output DIREKT
                    clean_code = raw_code.output
                    logger.info(
                        f"Extracted code from AgentRunResult.output: {len(str(clean_code))} characters"
                    )
                else:
                    # Fallback für String-Response
                    clean_code = str(raw_code)
                    logger.info(
                        f"Using raw_code as string: {len(clean_code)} characters"
                    )

                if clean_code and str(clean_code).strip():
                    logger.info("ETL-Code erfolgreich generiert und bereinigt")

                    # Enhanced Logging: AI Success
                    etl_core_logger.log_ai_interaction(
                        prompt=request.description,
                        response=clean_code,
                        tokens_used=0,  # TODO: Extract from raw_code if available
                        duration=ai_duration,
                    )

                    # Sichere ETLResponse-Erstellung - verhindert 'unhashable type: slice' Fehler
                    try:
                        # Schema_info robust konvertieren
                        safe_schema_info = {}
                        if schema_info:
                            for key, value in schema_info.items():
                                try:
                                    # Sichere Konvertierung von komplexen Objekten
                                    if isinstance(value, dict):
                                        safe_schema_info[key] = dict(value)
                                    elif isinstance(value, list):
                                        safe_schema_info[key] = list(value)
                                    else:
                                        safe_schema_info[key] = str(value)
                                except Exception as convert_error:
                                    logger.warning(
                                        f"Schema-Info Konvertierung für {key} fehlgeschlagen: {convert_error}"
                                    )
                                    safe_schema_info[key] = str(value)

                        return ETLResponse(
                            status="success",
                            generated_code=clean_code,
                            execution_plan=self._generate_execution_plan(
                                request, context
                            ),
                            metadata={
                                "model": self.llm_model_name,
                                "endpoint": self.llm_endpoint,
                                "context_connections": list(
                                    context.available_connections
                                ),  # Explizit zu Liste konvertieren
                                "generation_timestamp": datetime.now().isoformat(),
                                "tokens_used": 0,  # TODO: Extract from raw_code
                                "ai_duration": ai_duration,
                            },
                            # schema_info sicher hinzufügen wenn ETLResponse das unterstützt
                        )
                    except Exception as response_error:
                        etl_core_logger.log_error(
                            response_error, "ETLResponse creation failed"
                        )
                        logger.error(
                            f"ETLResponse-Erstellung fehlgeschlagen: {response_error}"
                        )
                        # Minimal-Response ohne problematische Felder
                        return ETLResponse(
                            status="success",
                            generated_code=clean_code,
                            execution_plan=["Code generiert"],
                            metadata={"model": self.llm_model_name},
                        )
                else:
                    return ETLResponse(
                        status="error", error_message="AI-Agent gab leeren Code zurück"
                    )

            except Exception as agent_error:
                logger.error(f"AI-Agent Fehler: {agent_error}")
                return ETLResponse(
                    status="error", error_message=f"AI-Agent Fehler: {str(agent_error)}"
                )

        except Exception as e:
            logger.error(f"Fehler bei ETL-Verarbeitung: {e}")
            return ETLResponse(status="error", error_message=str(e))

    async def _build_comprehensive_context(
        self, request: ETLRequest
    ) -> ETLCodeGenerationContext:
        """Sammelt umfassenden Kontext für Code-Generierung"""
        try:
            # Verbindungen als String-Liste konvertieren (KORRIGIERT)
            available_connections_raw = self.db_manager.list_connections()
            available_connections = []
            for conn in available_connections_raw:
                if isinstance(conn, dict):
                    available_connections.append(conn.get("name", str(conn)))
                else:
                    available_connections.append(str(conn))

            logger.info(f"Verfügbare Verbindungen konvertiert: {available_connections}")

            # Transformation hints basierend auf der Beschreibung ableiten
            transformation_hints = self._extract_transformation_hints(
                request.description
            )

            context = ETLCodeGenerationContext(
                available_connections=available_connections,
                transformation_hints=transformation_hints,
                performance_requirements=request.metadata.get("performance", {})
                if request.metadata
                else {},
            )

            logger.info(
                f"Kontext erstellt: {len(available_connections)} Verbindungen verfügbar"
            )
            return context

        except Exception as e:
            logger.error(f"Fehler beim Erstellen des Kontexts: {e}")
            return ETLCodeGenerationContext()

    async def _gather_schema_information(
        self, request: ETLRequest, context: ETLCodeGenerationContext
    ) -> Dict[str, Any]:
        """Sammelt Schema-Informationen für verfügbare Verbindungen"""
        schema_info = {
            "source_schemas": {},
            "target_schemas": {},
            "available_connections_details": {},
        }

        try:
            for conn_name in context.available_connections:
                try:
                    logger.info(f"=== SCHEMA DEBUG für {conn_name} ===")
                    # Schema-Informationen für jede Verbindung sammeln - KORRIGIERT
                    conn_config = self.db_manager.connection_configs.get(conn_name, {})
                    conn_type = conn_config.get("type", "unknown")

                    logger.info(f"Verbindungstyp: {conn_type}")

                    # Nutze die neue get_schema_info Methode
                    if conn_type == "mongodb":
                        # Für MongoDB: Extrahiere Datenbank aus Connection String
                        conn_string = conn_config.get("connection_string", "")
                        db_name = (
                            conn_string.split("/")[-1]
                            if "/" in conn_string
                            else "default"
                        )
                        logger.info(f"MongoDB Datenbank: {db_name}")
                        schema = self.db_manager.get_schema_info(
                            conn_name, database=db_name
                        )
                    else:
                        # Für SQL-Datenbanken
                        schema = self.db_manager.get_schema_info(conn_name)

                    logger.info(
                        f"Schema abgerufen: {len(schema.get('tables', schema.get('collections', {})))} Tabellen/Collections"
                    )

                    # Schema-Details loggen
                    if conn_type == "mongodb":
                        collections = schema.get("collections", {})
                        for coll_name, coll_info in collections.items():
                            logger.info(
                                f"  Collection {coll_name}: {coll_info.get('sample_fields', [])}"
                            )
                    else:
                        tables = schema.get("tables", {})
                        for table_name, table_info in tables.items():
                            try:
                                # Robuste Column-Extraktion - verhindert 'unhashable type: slice' Fehler
                                columns_data = table_info.get("columns", [])
                                if isinstance(columns_data, list):
                                    columns = []
                                    for col in columns_data:
                                        if isinstance(col, dict) and "name" in col:
                                            columns.append(str(col["name"]))
                                        elif isinstance(col, str):
                                            columns.append(col)
                                        else:
                                            columns.append(str(col))
                                else:
                                    columns = (
                                        [str(columns_data)] if columns_data else []
                                    )

                                logger.info(f"  Tabelle {table_name}: {columns}")
                            except Exception as col_error:
                                logger.warning(
                                    f"  Tabelle {table_name}: Schema-Fehler - {col_error}"
                                )
                                logger.info(
                                    f"  Tabelle {table_name}: [Schema nicht verfügbar]"
                                )

                    schema_info["available_connections_details"][conn_name] = {
                        "type": conn_type,
                        "schema": schema,
                        "status": "available",
                    }

                except Exception as e:
                    logger.warning(f"Konnte Schema für {conn_name} nicht laden: {e}")
                    schema_info["available_connections_details"][conn_name] = {
                        "status": "error",
                        "error": str(e),
                    }

            # Spezifische Source/Target Schema-Info wenn in Request definiert
            if request.source_config and "connection_name" in request.source_config:
                source_conn = request.source_config["connection_name"]
                if source_conn in schema_info["available_connections_details"]:
                    schema_info["source_schemas"][source_conn] = schema_info[
                        "available_connections_details"
                    ][source_conn]

            if request.target_config and "connection_name" in request.target_config:
                target_conn = request.target_config["connection_name"]
                if target_conn in schema_info["available_connections_details"]:
                    schema_info["target_schemas"][target_conn] = schema_info[
                        "available_connections_details"
                    ][target_conn]

        except Exception as e:
            logger.error(f"Fehler beim Sammeln der Schema-Informationen: {e}")

        return schema_info

    def _extract_transformation_hints(self, description: str) -> List[str]:
        """Extrahiert Transformations-Hinweise aus der Beschreibung"""
        hints = []
        description_lower = description.lower()

        # Häufige ETL-Operationen erkennen
        if "filter" in description_lower or "filtern" in description_lower:
            hints.append("Data filtering required")
        if "join" in description_lower or "verknüpfen" in description_lower:
            hints.append("Table joins required")
        if "aggregat" in description_lower or "gruppier" in description_lower:
            hints.append("Data aggregation required")
        if "transform" in description_lower or "transformier" in description_lower:
            hints.append("Data transformation required")
        if "csv" in description_lower:
            hints.append("CSV export required")
        if "excel" in description_lower:
            hints.append("Excel export required")
        if "chunk" in description_lower or "batch" in description_lower:
            hints.append("Chunked processing for large datasets")

        return hints

    def _build_enhanced_generation_prompt(
        self,
        request: ETLRequest,
        context: ETLCodeGenerationContext,
        schema_info: Dict[str, Any],
    ) -> str:
        """Erstellt erweiterten Prompt für Code-Generierung mit vollständiger DB-Kontext-Information"""

        prompt_parts = [
            f"AUFGABE: Erstelle ETL-Code für: {request.description}",
            "",
            "VOLLSTÄNDIGE DATENBANK-KONTEXT-INFORMATION:",
            "",
        ]

        # Verfügbare Verbindungen mit VOLLSTÄNDIGEN Details auflisten
        for conn_name in context.available_connections:
            conn_details = schema_info.get("available_connections_details", {}).get(
                conn_name, {}
            )
            conn_type = conn_details.get("type", "unknown")
            status = conn_details.get("status", "unknown")

            # Hole Connection-String aus DatabaseManager
            conn_config = self.db_manager.connection_configs.get(conn_name, {})
            conn_string = conn_config.get("connection_string", "nicht verfügbar")

            # DB-Typ-Klassifizierung
            db_category = (
                "SQL"
                if conn_type in ["mysql", "postgresql", "sqlite", "oracle", "sqlserver"]
                else "NoSQL"
            )

            prompt_parts.extend(
                [
                    f"DATABASE: {conn_name}",
                    f"  - Connection-String: {conn_string}",
                    f"  - DB-Typ: {conn_type}",
                    f"  - Kategorie: {db_category}",
                    f"  - Status: {status}",
                ]
            )

            # DB-spezifische Abfrage-Hinweise
            if db_category == "SQL":
                prompt_parts.extend(
                    [
                        "  - Query-Sprache: SQL",
                        "  - Konzept: Tabellen",
                        '  - Extract-Config: {"query": "SELECT * FROM table_name"}',
                        '  - Load-Config: {"table": "target_table", "if_exists": "replace"}',
                        f"  - Verwendung: db_manager.extract_data('{conn_name}', extract_config)",
                        f"  - Verwendung: db_manager.load_data('{conn_name}', dataframe, load_config)",
                    ]
                )
            else:  # NoSQL (MongoDB)
                prompt_parts.extend(
                    [
                        "  - Query-Sprache: MongoDB Query Language",
                        "  - Konzept: Collections",
                        '  - Extract-Config: {"database": "db_name", "collection": "collection_name", "query": {}}',
                        '  - Load-Config: {"collection": "target_collection", "operation": "replace"}',
                        f"  - Verwendung: db_manager.extract_data('{conn_name}', extract_config)",
                        f"  - Verwendung: db_manager.load_data('{conn_name}', dataframe, load_config)",
                        "  - WICHTIG: Für MongoDB verwende 'collection' statt 'table' im Load-Config!",
                        "  - WICHTIG: Database wird automatisch aus Connection String erkannt!",
                    ]
                )

            # Schema-Details hinzufügen wenn verfügbar
            if "schema" in conn_details and conn_details["schema"]:
                schema = conn_details["schema"]
                if isinstance(schema, dict):
                    if "tables" in schema and schema["tables"]:
                        # SQL-Tabellen
                        tables = schema["tables"]
                        table_names = (
                            list(tables.keys())
                            if isinstance(tables, dict)
                            else list(tables)
                        )
                        table_display = table_names[:3]
                        table_str = ", ".join(str(t) for t in table_display)
                        more_indicator = (
                            f" (+ {len(table_names) - 3} weitere)"
                            if len(table_names) > 3
                            else ""
                        )
                        prompt_parts.append(
                            f"  - Verfügbare Tabellen: {table_str}{more_indicator}"
                        )

                        # Beispiel-Tabelle mit Spalten
                        if table_names and isinstance(tables, dict):
                            first_table = table_names[0]
                            table_info = tables.get(first_table, {})
                            columns_data = table_info.get("columns", [])
                            if columns_data:
                                columns = []
                                for col in columns_data[:5]:  # Nur erste 5 Spalten
                                    if isinstance(col, dict) and "name" in col:
                                        columns.append(str(col["name"]))
                                    else:
                                        columns.append(str(col))
                                if columns:
                                    prompt_parts.append(
                                        f"  - Beispiel-Spalten ({first_table}): {', '.join(columns)}"
                                    )

                    elif "collections" in schema and schema["collections"]:
                        # MongoDB-Collections
                        collections = schema["collections"]
                        coll_names = (
                            list(collections.keys())
                            if isinstance(collections, dict)
                            else list(collections)
                        )
                        coll_display = coll_names[:3]
                        coll_str = ", ".join(str(c) for c in coll_display)
                        more_indicator = (
                            f" (+ {len(coll_names) - 3} weitere)"
                            if len(coll_names) > 3
                            else ""
                        )
                        prompt_parts.append(
                            f"  - Verfügbare Collections: {coll_str}{more_indicator}"
                        )

                        # Beispiel-Collection mit Feldern
                        if coll_names and isinstance(collections, dict):
                            first_coll = coll_names[0]
                            coll_info = collections.get(first_coll, {})
                            sample_fields = coll_info.get("sample_fields", [])
                            if sample_fields:
                                fields_str = ", ".join(
                                    str(f) for f in sample_fields[:5]
                                )
                                prompt_parts.append(
                                    f"  - Beispiel-Felder ({first_coll}): {fields_str}"
                                )

            prompt_parts.append("")  # Leerzeile zwischen DBs

        prompt_parts.extend(
            [
                "TRANSFORMATIONS-HINWEISE:",
                *[f"- {hint}" for hint in context.transformation_hints],
                "",
                "SPEZIFISCHE ANFORDERUNGEN:",
            ]
        )

        # Source/Target Config hinzufügen
        if request.source_config:
            prompt_parts.append(f"- Quelle: {request.source_config}")
        if request.target_config:
            prompt_parts.append(f"- Ziel: {request.target_config}")
        if request.transformation_rules:
            prompt_parts.append(
                f"- Transformationsregeln: {request.transformation_rules}"
            )

        prompt_parts.extend(
            [
                "",
                "CROSS-PLATFORM ETL-HINWEISE:",
                "- Für SQL -> NoSQL: Verwende DataFrame als Zwischenschritt",
                "- Für NoSQL -> SQL: Normalisiere Datenstruktur",
                "- Für SQL -> SQL: Direkte Übertragung möglich",
                "- Für NoSQL -> NoSQL: Strukturelle Anpassungen beachten",
                "",
                "WICHTIGE IMPLEMENTIERUNGSREGELN:",
                "- VERWENDE IMMER die ursprünglichen Connection-Namen (z.B. 'Jup', 'mong4')",
                "- Für SQL-Datenbanken: load_config = {'table': 'target_table', 'if_exists': 'replace'}",
                "- Für MongoDB: load_config = {'collection': 'target_collection', 'operation': 'replace'}",
                "- NIEMALS 'table' für MongoDB verwenden - nur 'collection'!",
                "- NIEMALS 'database' Parameter - wird automatisch aus Connection String erkannt!",
                "- Connection-Namen exakt wie in der Datenbank-Konfiguration verwenden",
                "- Code muss EIGENSTÄNDIG sein - keine etl_agent Imports!",
                "",
                "EIGENSTÄNDIGER CODE-ANFORDERUNGEN:",
                "- Verwende SimpleDBManager-Klasse (im Code definiert)",
                "- Nur Standard-Libraries: pandas, pymongo, sqlalchemy, json, os",
                "- Code muss auf jedem Server ohne ETL-Agent laufen",
                "- Vollständige Implementierung in einer Datei",
                "",
                "GENERIERE VOLLSTÄNDIGEN PYTHON ETL-CODE:",
                "- Verwende DatabaseManager für alle DB-Operationen",
                "- Implementiere robuste Fehlerbehandlung",
                "- Füge detailliertes Logging hinzu",
                "- Optimiere für Performance",
                "- Code muss sofort ausführbar sein",
                "- KEINE FALLBACKS - nur präzise DB-spezifische Operationen",
                "",
                "NUR PYTHON-CODE, KEINE MARKDOWN-BLÖCKE!",
            ]
        )

        return "\n".join(prompt_parts)

    def _generate_execution_plan(
        self, request: ETLRequest, context: ETLCodeGenerationContext
    ) -> List[str]:
        """Generiert Ausführungsplan basierend auf Request und Context"""
        plan = [
            "1. DatabaseManager initialisieren",
            "2. Verbindungen validieren",
            "3. Extract: Daten aus Quelle laden",
        ]

        # Transform-Schritte basierend auf Hints
        for hint in context.transformation_hints:
            if "filtering" in hint:
                plan.append("4. Transform: Daten filtern")
            elif "join" in hint:
                plan.append("4. Transform: Tabellen verknüpfen")
            elif "aggregation" in hint:
                plan.append("4. Transform: Daten aggregieren")
            else:
                plan.append("4. Transform: Daten transformieren")

        plan.extend(
            [
                "5. Load: Transformierte Daten laden",
                "6. Verbindungen schließen",
                "7. Cleanup und Logging",
            ]
        )

        return plan

    def _clean_and_format_code(self, raw_code: str) -> str:
        """
        Minimale Code-Bereinigung - nur das Nötigste
        """
        if raw_code is None:
            return ""

        # Direkte Rückgabe des Codes ohne Bereinigung
        return str(raw_code).strip()


def create_etl_agent() -> ETLAgent:
    """Factory-Funktion für ETL-Agent Erstellung"""
    return ETLAgent()


def create_a2a_compatible_agent() -> object:
    """Erstellt A2A-kompatiblen ETL-Agent"""
    agent = create_etl_agent()
    return agent.to_a2a()


# Für A2A-Integration
if __name__ == "__main__":
    # Kann direkt als A2A-Agent verwendet werden
    agent = create_etl_agent()
    app = agent.to_a2a()
