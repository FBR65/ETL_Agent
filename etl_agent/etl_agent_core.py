"""
ETL Agent Core Module mit PydanticAI und MCP/A2A Integration
"""

import os
import logging
from typing import Dict, List, Any, Optional, Union
from pydantic import BaseModel, Field
from pydantic_ai import Agent
from pydantic_ai.models.openai import OpenAIModel
from pydantic_ai.providers.openai import OpenAIProvider

from .database_manager import DatabaseManager

logger = logging.getLogger(__name__)


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


class ETLAgent:
    """Hauptklasse für den ETL-Agent mit Multi-Database Support"""

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """Initialisiert den ETL-Agent"""
        self.db_manager = db_manager or DatabaseManager()
        self.setup_llm()
        self.setup_agent()

    def setup_llm(self):
        """LLM-Konfiguration"""
        self.llm_endpoint = os.getenv("BASE_URL", "http://localhost:11434/v1")
        self.llm_api_key = os.getenv("API_KEY", "ollama")
        self.llm_model_name = os.getenv("MODEL_NAME", "qwen2.5:latest")

        # OpenAI-kompatible Provider-Konfiguration
        self.provider = OpenAIProvider(
            base_url=self.llm_endpoint, api_key=self.llm_api_key
        )
        self.model = OpenAIModel(provider=self.provider, model_name=self.llm_model_name)

    def setup_agent(self):
        """PydanticAI Agent konfigurieren"""
        system_prompt = """Du bist ein spezialisierter ETL (Extract, Transform, Load) Agent.

Deine Aufgaben:
1. Natürlichsprachige ETL-Beschreibungen in Python-Code umwandeln
2. Multi-Datenbankverbindungen verwalten (MongoDB, PostgreSQL, MySQL, Oracle, etc.)
3. Transformationslogik generieren mit pandas
4. Fehlerbehandlung und Logging implementieren
5. Optimierte ETL-Pipelines erstellen

Antworte immer auf Deutsch und generiere sauberen, dokumentierten Python-Code."""

        self.agent = Agent(
            model=self.model,
            result_type=ETLResponse,
            retries=3,
            system_prompt=system_prompt,
        )

    async def process_etl_request(self, request: ETLRequest) -> ETLResponse:
        """Verarbeitet eine ETL-Anfrage mit Multi-DB Support"""
        try:
            logger.info(f"Verarbeite ETL-Anfrage: {request.description}")

            # 1. Schritt: Anfrage analysieren
            context = await self._analyze_request(request)

            # 2. Schritt: Datenbankschemas abrufen
            schema_info = await self._get_schema_information(request)
            context.update(schema_info)

            # 3. Schritt: ETL-Code generieren
            prompt = self._build_generation_prompt(request, context)

            # Agent ausführen
            result = await self.agent.run(prompt)

            logger.info("ETL-Code erfolgreich generiert")
            return result

        except Exception as e:
            logger.error(f"Fehler bei ETL-Verarbeitung: {e}")
            return ETLResponse(status="error", error_message=str(e))

    async def _analyze_request(self, request: ETLRequest) -> Dict[str, Any]:
        """Analysiert die ETL-Anfrage mit verbesserter Intent-Erkennung"""
        context = {
            "description": request.description,
            "has_source_config": request.source_config is not None,
            "has_target_config": request.target_config is not None,
            "transformation_rules_count": len(request.transformation_rules or []),
            "available_connections": self.db_manager.list_connections(),
        }

        # Erweiterte Intent-Erkennung
        description_lower = request.description.lower()

        # Datenbanktyp-Erkennung
        if any(
            word in description_lower
            for word in ["mongodb", "nosql", "collection", "document"]
        ):
            context["source_type"] = "mongodb"
        elif any(
            word in description_lower
            for word in ["sql", "tabelle", "table", "mysql", "postgres"]
        ):
            context["source_type"] = "sql"
        else:
            context["source_type"] = "mixed"

        # ETL-Operationen erkennen
        operations = []
        if any(word in description_lower for word in ["filter", "where", "bedingung"]):
            operations.append("filter")
        if any(word in description_lower for word in ["join", "verknüpf", "verbind"]):
            operations.append("join")
        if any(
            word in description_lower
            for word in ["aggreg", "sum", "count", "group", "gruppier"]
        ):
            operations.append("aggregate")
        if any(
            word in description_lower
            for word in ["transform", "umwandel", "konvertier"]
        ):
            operations.append("transform")
        if any(word in description_lower for word in ["clean", "bereinig", "säuber"]):
            operations.append("clean")

        context["detected_operations"] = operations

        return context

    async def _get_schema_information(self, request: ETLRequest) -> Dict[str, Any]:
        """Sammelt Schema-Informationen aus verfügbaren Datenbanken"""
        schema_info = {
            "source_schema": None,
            "target_schema": None,
            "available_connections": self.db_manager.list_connections(),
        }

        # Schema-Info für Quell-Verbindung
        if request.source_config and "connection_name" in request.source_config:
            try:
                conn_name = request.source_config["connection_name"]
                schema = self.db_manager.get_database_schema(conn_name)
                schema_info["source_schema"] = {
                    "connection": conn_name,
                    "schema": schema,
                }
            except Exception as e:
                logger.warning(f"Quell-Schema-Info Fehler: {e}")

        # Schema-Info für Ziel-Verbindung
        if request.target_config and "connection_name" in request.target_config:
            try:
                conn_name = request.target_config["connection_name"]
                schema = self.db_manager.get_database_schema(conn_name)
                schema_info["target_schema"] = {
                    "connection": conn_name,
                    "schema": schema,
                }
            except Exception as e:
                logger.warning(f"Ziel-Schema-Info Fehler: {e}")

        return schema_info

    def _build_generation_prompt(
        self, request: ETLRequest, context: Dict[str, Any]
    ) -> str:
        """Erstellt detaillierten Prompt für Code-Generierung"""
        prompt_parts = [
            f"ETL-Aufgabe: {request.description}",
            "",
            "=== KONTEXT-INFORMATIONEN ===",
        ]

        # Verfügbare Verbindungen
        if context.get("available_connections"):
            prompt_parts.append("Verfügbare Datenbankverbindungen:")
            for conn in context["available_connections"]:
                prompt_parts.append(
                    f"- {conn['name']} ({conn['type']}) - Status: {conn['status']}"
                )
            prompt_parts.append("")

        # Erkannte Operationen
        if context.get("detected_operations"):
            prompt_parts.append(
                f"Erkannte ETL-Operationen: {', '.join(context['detected_operations'])}"
            )
            prompt_parts.append("")

        # Schema-Informationen
        if context.get("source_schema"):
            prompt_parts.append("=== QUELL-SCHEMA ===")
            source_schema = context["source_schema"]["schema"]
            if isinstance(source_schema, dict):
                for key, value in source_schema.items():
                    prompt_parts.append(f"{key}: {value}")
            prompt_parts.append("")

        if context.get("target_schema"):
            prompt_parts.append("=== ZIEL-SCHEMA ===")
            target_schema = context["target_schema"]["schema"]
            if isinstance(target_schema, dict):
                for key, value in target_schema.items():
                    prompt_parts.append(f"{key}: {value}")
            prompt_parts.append("")

        # Code-Anforderungen
        prompt_parts.extend(
            [
                "=== CODE-ANFORDERUNGEN ===",
                "Generiere eine vollständige ETL-Pipeline mit:",
                "1. DatabaseManager-Integration für Multi-DB-Support",
                "2. Robuste Fehlerbehandlung mit try/except",
                "3. Detailliertes Logging aller Schritte",
                "4. Pandas für Datenmanipulation",
                "5. Dokumentierte Funktionen mit Docstrings",
                "6. Performance-Optimierungen (Chunking bei großen Daten)",
                "",
                "Verwende diese Code-Struktur:",
                "```python",
                "import pandas as pd",
                "import logging",
                "from database_manager import DatabaseManager",
                "",
                "logger = logging.getLogger(__name__)",
                "",
                "def etl_pipeline():",
                '    """Generierte ETL-Pipeline basierend auf Benutzeranfrage"""',
                "    db_manager = DatabaseManager()",
                "    ",
                "    try:",
                "        # Implementierung hier",
                "        pass",
                "    except Exception as e:",
                "        logger.error(f'ETL-Pipeline Fehler: {e}')",
                "        raise",
                "",
                "if __name__ == '__main__':",
                "    etl_pipeline()",
                "```",
                "",
                "Implementiere die spezifische Logik basierend auf der Aufgabenbeschreibung.",
            ]
        )

        return "\n".join(prompt_parts)

    def to_a2a(self):
        """A2A-kompatible App erstellen"""
        return self.agent.to_a2a()
