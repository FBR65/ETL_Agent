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

# MongoDB-Tools aus dataanalyser übernehmen
from tools.mongodb_connector import MongoDBConnector
from tools.database_connector import DatabaseConnector  # Wird erstellt
from tools.etl_processor import ETLProcessor  # Wird erstellt

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
    """Hauptklasse für den ETL-Agent"""

    def __init__(self):
        """Initialisiert den ETL-Agent"""
        self.setup_llm()
        self.setup_database_connectors()
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

    def setup_database_connectors(self):
        """Datenbank-Verbindungen initialisieren"""
        self.mongodb_connector = None
        self.sql_connector = None

        # MongoDB Connector
        mongo_conn_str = os.getenv(
            "MONGO_CONNECTION_STRING", "mongodb://localhost:27017/"
        )
        try:
            self.mongodb_connector = MongoDBConnector(
                connection_string=mongo_conn_str,
                openai_api_key=self.llm_api_key,
                openai_api_base=self.llm_endpoint,
                llm_model_name=self.llm_model_name,
            )
            logger.info("MongoDB Connector initialisiert")
        except Exception as e:
            logger.warning(f"MongoDB Connector Fehler: {e}")

    def setup_agent(self):
        """PydanticAI Agent konfigurieren"""
        system_prompt = """Du bist ein spezialisierter ETL (Extract, Transform, Load) Agent.

Deine Aufgaben:
1. Natürlichsprachige ETL-Beschreibungen in Python-Code umwandeln
2. Datenbankverbindungen und Schemas analysieren
3. Transformationslogik generieren
4. Fehlerbehandlung implementieren
5. Optimierte ETL-Pipelines erstellen

Verfügbare Tools:
- MongoDB-Connector für NoSQL-Datenbanken
- SQL-Connector für relationale Datenbanken
- Metadaten-Extraktion
- Code-Generierung mit Best Practices

Antworte immer auf Deutsch und generiere sauberen, dokumentierten Python-Code."""

        self.agent = Agent(
            model=self.model,
            result_type=ETLResponse,
            retries=3,
            system_prompt=system_prompt,
        )

    async def process_etl_request(self, request: ETLRequest) -> ETLResponse:
        """Verarbeitet eine ETL-Anfrage"""
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
        """Analysiert die ETL-Anfrage"""
        context = {
            "description": request.description,
            "has_source_config": request.source_config is not None,
            "has_target_config": request.target_config is not None,
            "transformation_rules_count": len(request.transformation_rules or []),
        }

        # Intent-Erkennung (vereinfacht)
        description_lower = request.description.lower()

        if "mongodb" in description_lower or "nosql" in description_lower:
            context["database_type"] = "mongodb"
        elif (
            "sql" in description_lower
            or "mysql" in description_lower
            or "postgres" in description_lower
        ):
            context["database_type"] = "sql"
        else:
            context["database_type"] = "mixed"

        if "filter" in description_lower or "where" in description_lower:
            context["needs_filtering"] = True

        if "join" in description_lower or "verknüpf" in description_lower:
            context["needs_joins"] = True

        if (
            "aggreg" in description_lower
            or "sum" in description_lower
            or "count" in description_lower
        ):
            context["needs_aggregation"] = True

        return context

    async def _get_schema_information(self, request: ETLRequest) -> Dict[str, Any]:
        """Sammelt Schema-Informationen aus den Datenbanken"""
        schema_info = {
            "source_schema": None,
            "target_schema": None,
            "available_tables": [],
            "available_collections": [],
        }

        # MongoDB Schema-Info
        if self.mongodb_connector and request.source_config:
            try:
                if request.source_config.get("type") == "mongodb":
                    db_name = request.source_config.get("database")
                    if db_name:
                        collections = self.mongodb_connector.list_collections(db_name)
                        schema_info["available_collections"] = collections

                        # Struktur der ersten Collection als Beispiel
                        if collections:
                            structure = self.mongodb_connector.get_collection_structure(
                                db_name, collections[0], sample_size=3
                            )
                            schema_info["sample_structure"] = structure
            except Exception as e:
                logger.warning(f"Schema-Info Fehler: {e}")

        return schema_info

    def _build_generation_prompt(
        self, request: ETLRequest, context: Dict[str, Any]
    ) -> str:
        """Erstellt den Prompt für die Code-Generierung"""
        prompt_parts = [
            f"ETL-Aufgabe: {request.description}",
            "",
            "Kontext-Informationen:",
        ]

        for key, value in context.items():
            prompt_parts.append(f"- {key}: {value}")

        prompt_parts.extend(
            [
                "",
                "Generiere Python-Code für diese ETL-Pipeline mit:",
                "1. Datenbankverbindungen",
                "2. Datenextraktion",
                "3. Transformationslogik",
                "4. Datenladung",
                "5. Fehlerbehandlung",
                "6. Logging",
                "",
                "Verwende verfügbare Bibliotheken: pandas, pymongo, sqlalchemy",
                "Dokumentiere den Code ausführlich.",
            ]
        )

        return "\n".join(prompt_parts)

    def to_a2a(self):
        """A2A-kompatible App erstellen"""
        return self.agent.to_a2a()


# Standalone Funktionen für direkte Nutzung
async def create_etl_agent() -> ETLAgent:
    """Factory-Funktion für ETL-Agent"""
    return ETLAgent()


async def process_etl_description(
    description: str,
    source_config: Optional[Dict] = None,
    target_config: Optional[Dict] = None,
) -> ETLResponse:
    """Vereinfachte ETL-Verarbeitung"""
    agent = await create_etl_agent()
    request = ETLRequest(
        description=description,
        source_config=source_config,
        target_config=target_config,
    )
    return await agent.process_etl_request(request)
