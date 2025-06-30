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

        self.provider = OpenAIProvider(
            base_url=self.llm_endpoint, api_key=self.llm_api_key
        )
        self.model = OpenAIModel(provider=self.provider, model_name=self.llm_model_name)

    def setup_agent(self):
        """PydanticAI Agent konfigurieren"""
        system_prompt = """Du bist ein ETL Code-Generator. 
Generiere vollständigen, ausführbaren Python ETL-Code.
Antworte NUR mit dem Python-Code, keine Markdown-Blöcke."""

        self.agent = Agent(
            model=self.model,
            result_type=str,
            retries=0,
            system_prompt=system_prompt,
        )

    async def process_etl_request(self, request: ETLRequest) -> ETLResponse:
        """Verarbeitet eine ETL-Anfrage mit Multi-DB Support"""
        try:
            logger.info(f"Verarbeite ETL-Anfrage: {request.description}")

            context = await self._analyze_request(request)
            schema_info = await self._get_schema_information(request)
            context.update(schema_info)
            prompt = self._build_generation_prompt(request, context)

            try:
                logger.info("Starte AI-Agent für Code-Generierung...")
                raw_code = await self.agent.run(prompt)
                clean_code = self._clean_and_format_code(str(raw_code))

                if clean_code.strip():
                    logger.info("ETL-Code erfolgreich generiert und bereinigt")
                    return ETLResponse(
                        status="success",
                        generated_code=clean_code,
                        execution_plan=["Extract", "Transform", "Load"],
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

    async def _analyze_request(self, request: ETLRequest) -> Dict[str, Any]:
        """Analysiert die ETL-Anfrage"""
        return {
            "description": request.description,
            "available_connections": self.db_manager.list_connections(),
        }

    async def _get_schema_information(self, request: ETLRequest) -> Dict[str, Any]:
        """Sammelt Schema-Informationen"""
        return {
            "source_schema": None,
            "target_schema": None,
            "available_connections": self.db_manager.list_connections(),
        }

    def _build_generation_prompt(
        self, request: ETLRequest, context: Dict[str, Any]
    ) -> str:
        """Erstellt Prompt für Code-Generierung"""
        return f"Erstelle ETL-Code für: {request.description}"

    def _clean_and_format_code(self, raw_code: str) -> str:
        """Bereinigt den generierten Code"""
        clean_code = raw_code.strip()
        if clean_code.startswith("```python"):
            clean_code = clean_code[9:]
        if clean_code.startswith("```"):
            clean_code = clean_code[3:]
        if clean_code.endswith("```"):
            clean_code = clean_code[:-3]
        return clean_code.strip()
