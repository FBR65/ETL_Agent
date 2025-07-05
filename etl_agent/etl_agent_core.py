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
        self.llm_model_name = os.getenv("MODEL_NAME", "qwen2.5:latest")

        logger.info(f"Initialisiere LLM: {self.llm_model_name} @ {self.llm_endpoint}")

        self.provider = OpenAIProvider(
            base_url=self.llm_endpoint, api_key=self.llm_api_key
        )
        self.model = OpenAIModel(provider=self.provider, model_name=self.llm_model_name)

    def setup_agent(self):
        """PydanticAI Agent konfigurieren mit verbessertem System-Prompt"""
        system_prompt = """Du bist ein Experte für ETL (Extract, Transform, Load) Prozesse.

AUFGABE: Generiere vollständigen, ausführbaren Python ETL-Code basierend auf natürlichsprachigen Beschreibungen.

WICHTIG: Du bekommst ECHTE Schema-Informationen aus den Datenbanken. NUTZE DIESE!

SCHEMA-INFORMATIONEN NUTZEN:
1. Schaue in database_schemas -> [connection_name] -> tables/collections
2. Verwende ECHTE Tabellennamen und Spaltennamen
3. Für SQL: Nutze schema.tables[table_name].columns[].name
4. Für MongoDB: Nutze schema.collections[collection_name].sample_fields

DATABASE MANAGER API (EXAKT SO VERWENDEN):
- Daten extrahieren: db_manager.extract_data(connection_name, query_config)
- Daten laden: db_manager.load_data(connection_name, dataframe, load_config)

SQL Query Config:
{"query": "SELECT column1, column2 FROM table_name WHERE condition"}

MongoDB Query Config:
{"database": "db_name", "collection": "collection_name", "query": {}, "limit": 1000}

Load Config:
{"table": "target_table_name", "if_exists": "replace"}  # oder "append"

SCHEMA-FIRST ANSATZ:
1. ZUERST: Analysiere verfügbare Tabellen/Collections aus schema_context
2. DANN: Wähle passende Tabellen für die ETL-Beschreibung
3. DANACH: Verwende echte Spaltennamen in SQL/MongoDB Queries
4. SCHLIESSLICH: Generiere Code mit echten Daten

CODE-STRUKTUR:
```python
import pandas as pd
import logging
from etl_agent.database_manager import DatabaseManager

logger = logging.getLogger(__name__)

def etl_pipeline():
    try:
        db_manager = DatabaseManager()
        
        # EXTRACT mit echten Tabellennamen
        df = db_manager.extract_data("connection_name", {"query": "SELECT real_columns FROM real_table"})
        
        # TRANSFORM
        # Datenverarbeitung hier
        
        # LOAD  
        db_manager.load_data("target_connection", df, {"table": "target_table", "if_exists": "replace"})
        
        logger.info("ETL abgeschlossen")
        return df
    except Exception as e:
        logger.error(f"ETL Fehler: {e}")
        raise

if __name__ == "__main__":
    etl_pipeline()
```

ANTWORTE NUR MIT DEM PYTHON-CODE, KEINE MARKDOWN-BLÖCKE!"""

        self.agent = Agent(
            model=self.model,
            result_type=str,
            retries=3,
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
                logger.info(f"RAW AI RESPONSE: {raw_code}")
                logger.info("=== AI-AGENT DEBUG END ===")

                # AgentRunResult richtig verarbeiten
                if hasattr(raw_code, "output"):
                    # PydanticAI AgentRunResult - extrahiere output
                    code_content = raw_code.output
                    logger.info(
                        f"Extracted code from AgentRunResult.output: {len(str(code_content))} characters"
                    )
                else:
                    # Fallback für String-Response
                    code_content = str(raw_code)

                clean_code = self._clean_and_format_code(code_content)

                if clean_code.strip():
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
        """Erstellt erweiterten Prompt für Code-Generierung"""

        prompt_parts = [
            f"AUFGABE: Erstelle ETL-Code für: {request.description}",
            "",
            "VERFÜGBARE DATENBANKVERBINDUNGEN:",
        ]

        # Verfügbare Verbindungen mit Details auflisten
        for conn_name in context.available_connections:
            conn_details = schema_info.get("available_connections_details", {}).get(
                conn_name, {}
            )
            conn_type = conn_details.get("type", "unknown")
            status = conn_details.get("status", "unknown")
            prompt_parts.append(f"- {conn_name} (Typ: {conn_type}, Status: {status})")

            # Schema-Details hinzufügen wenn verfügbar
            if "schema" in conn_details and conn_details["schema"]:
                schema = conn_details["schema"]
                if isinstance(schema, dict) and "tables" in schema:
                    tables = schema["tables"]
                    if tables:
                        # Sichere Tabellen-Liste - verhindert 'unhashable type: slice' Fehler
                        table_names = (
                            list(tables.keys())
                            if isinstance(tables, dict)
                            else list(tables)
                        )
                        table_display = table_names[:5]
                        table_str = ", ".join(str(t) for t in table_display)
                        more_indicator = "..." if len(table_names) > 5 else ""
                        prompt_parts.append(f"  Tabellen: {table_str}{more_indicator}")

        prompt_parts.extend(
            [
                "",
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
                "GENERIERE VOLLSTÄNDIGEN PYTHON ETL-CODE:",
                "- Verwende DatabaseManager für alle DB-Operationen",
                "- Implementiere robuste Fehlerbehandlung",
                "- Füge detailliertes Logging hinzu",
                "- Optimiere für Performance",
                "- Code muss sofort ausführbar sein",
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
        Bereinigt und formatiert den generierten Code
        ✅ Behandelt AgentRunResult-Output korrekt
        ✅ Entfernt Markdown-Blöcke
        ✅ Formatiert für Lesbarkeit
        """
        # Sichere String-Konvertierung
        if raw_code is None:
            return ""

        clean_code = str(raw_code).strip()

        # AgentRunResult-spezifische Bereinigung
        if "AgentRunResult(output=" in clean_code:
            # Extrahiere Code aus AgentRunResult-String
            import re

            match = re.search(r"AgentRunResult\(output='([^']*)'", clean_code)
            if match:
                clean_code = match.group(1)
            else:
                # Fallback: versuche anderen Pattern
                if "output='" in clean_code:
                    start = clean_code.find("output='") + 8
                    end = clean_code.rfind("')")
                    if start < end:
                        clean_code = clean_code[start:end]

        # Escape-Sequenzen dekodieren
        clean_code = (
            clean_code.replace("\\n", "\n").replace("\\t", "\t").replace("\\'", "'")
        )

        # Markdown-Code-Blöcke entfernen
        if clean_code.startswith("```python"):
            clean_code = clean_code[9:]
        elif clean_code.startswith("```"):
            clean_code = clean_code[3:]

        if clean_code.endswith("```"):
            clean_code = clean_code[:-3]

        # Zusätzliche Bereinigung
        clean_code = clean_code.strip()

        # Import pandas hinzufügen wenn noch nicht vorhanden
        if "import pandas" not in clean_code and "pd." in clean_code:
            clean_code = "import pandas as pd\n" + clean_code

        return clean_code


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
