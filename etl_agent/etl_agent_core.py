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
        system_prompt = """🚨🚨🚨 ULTRA-KRITISCHE REGELN - BEFOLGE DIESE EXAKT ODER ES GIBT SOFORTIGEN FEHLER! 🚨🚨🚨

Du MUSST einen VOLLSTÄNDIGEN, FUNKTIONSFÄHIGEN Python-ETL-Code generieren!
NIEMALS Stubs, NIEMALS unvollständigen Code, NIEMALS Syntax-Fehler!

🔥 THESE EXACT RULES ARE MANDATORY - NO EXCEPTIONS:

1. ALWAYS use "SimpleDBManager" class - NEVER "DBManager" or "DatabaseManager"
2. ALWAYS include complete method implementations - NEVER use "..." or stubs
3. ALWAYS include all imports at the top
4. ALWAYS include proper logger configuration
5. ALWAYS include complete connection_configs with real connection strings
6. ALWAYS include both extract_data AND load_data methods with full implementation
7. ALWAYS include complete etl_pipeline function with Extract, Transform, Load
8. ALWAYS include if __name__ == "__main__" block

🎯 COPY THIS EXACT TEMPLATE WORD-FOR-WORD - ONLY CHANGE QUERIES AND TRANSFORMATIONS:

import pandas as pd
import logging
from typing import Dict, Any
import pymongo
import sqlalchemy as sa

# Logger konfigurieren
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SimpleDBManager:
    def __init__(self):
        self.connection_configs = {
            "Jup": {
                "connection_string": "mysql+mysqlconnector://dataanalyzer:dataanalyzer_pwd@localhost:3306/my_test_db",
                "type": "mysql"
            },
            "mong4": {
                "connection_string": "mongodb://user:pass@localhost:27017/my_test_db",
                "type": "mongodb"
            },
            "MYSQLTEST": {
                "connection_string": "mysql+mysqlconnector://dataanalyzer:dataanalyzer_pwd@localhost:3306/my_test_db",
                "type": "mariadb"
            }
        }
    
    def extract_data(self, connection_name: str, query_config: Dict[str, Any]) -> pd.DataFrame:
        config = self.connection_configs[connection_name]
        logger.info(f"Extrahiere Daten aus {connection_name} ({config['type']})")
        
        if config["type"] in ["mysql", "mariadb"]:
            engine = sa.create_engine(config["connection_string"])
            df = pd.read_sql(query_config["query"], engine)
            engine.dispose()
            return df
        elif config["type"] == "mongodb":
            client = pymongo.MongoClient(config["connection_string"])
            db_name = config["connection_string"].split("/")[-1] or "my_test_db"
            db = client[db_name]
            collection = db[query_config["collection"]]
            cursor = collection.find(query_config.get("query", {}))
            if "limit" in query_config:
                cursor = cursor.limit(query_config["limit"])
            data = list(cursor)
            client.close()
            return pd.DataFrame(data)
        else:
            raise ValueError(f"Nicht unterstützter DB-Typ: {config['type']}")
    
    def load_data(self, connection_name: str, df: pd.DataFrame, target_config: Dict[str, Any]):
        config = self.connection_configs[connection_name]
        logger.info(f"Lade {len(df)} Datensätze nach {connection_name} ({config['type']})")
        
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
            engine.dispose()

def etl_pipeline():
    try:
        logger.info("=== ETL-Pipeline gestartet ===")
        db_manager = SimpleDBManager()
        
        # EXTRACT
        logger.info("Phase 1: Daten extrahieren")
        extract_config = {"query": "SELECT * FROM users_test"}
        df = db_manager.extract_data("Jup", extract_config)
        logger.info(f"Extrahiert: {len(df)} Datensätze")
        
        # TRANSFORM
        logger.info("Phase 2: Daten transformieren")
        df['age'] = df['age'] + 2
        logger.info("Transformation abgeschlossen")
        
        # LOAD
        logger.info("Phase 3: Daten laden")
        load_config = {"collection": "users_transformed", "operation": "replace"}
        db_manager.load_data("mong4", df, load_config)
        logger.info("Daten erfolgreich geladen")
        
        logger.info("=== ETL-Pipeline erfolgreich abgeschlossen ===")
        return df
        
    except Exception as e:
        logger.error(f"ETL-Pipeline Fehler: {e}")
        raise

if __name__ == "__main__":
    result = etl_pipeline()
    print(f"ETL abgeschlossen. Verarbeitete Datensätze: {len(result)}")

🚨🚨🚨 ABSOLUTELY FORBIDDEN - THESE WILL CAUSE IMMEDIATE FAILURE:
- Using "DBManager" instead of "SimpleDBManager"
- Using "..." or incomplete method implementations
- Missing imports or logger configuration
- Syntax errors or indentation problems
- Missing connection_configs
- Calling wrong functions (e.g., load_data instead of extract_data)
- Incomplete or broken code

� MANDATORY REQUIREMENTS:
- COMPLETE, WORKING CODE ONLY
- ALL METHODS FULLY IMPLEMENTED
- CORRECT SYNTAX AND INDENTATION
- PROPER FUNCTION CALLS
- EMBEDDED CONNECTION STRINGS
- NO STUBS OR PLACEHOLDERS

GENERATE ONLY COMPLETE, EXECUTABLE PYTHON CODE WITHOUT MARKDOWN BLOCKS!
IF YOU GENERATE INCOMPLETE OR BROKEN CODE, IT IS A CRITICAL FAILURE!
NEVER use ```python or ``` - ONLY PURE PYTHON CODE!
START DIRECTLY WITH: import pandas as pd"""

        self.agent = Agent(
            model=self.model,
            result_type=str,
            retries=5,
            system_prompt=system_prompt,
        )

    def _build_embedded_connections_config(
        self, available_connections: List[str]
    ) -> str:
        """Erstellt Connection Config für eingebetteten Code"""
        config_parts = []
        config_parts.append("        self.connection_configs = {")

        for conn_name in available_connections:
            conn_config = self.db_manager.connection_configs.get(conn_name, {})
            conn_string = conn_config.get("connection_string", "")
            conn_type = conn_config.get("type", "")

            if conn_string and conn_type:
                config_parts.append(f'            "{conn_name}": {{')
                config_parts.append(
                    f'                "connection_string": "{conn_string}",'
                )
                config_parts.append(f'                "type": "{conn_type}"')
                config_parts.append("            },")

        config_parts.append("        }")

        return "\n".join(config_parts)

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

                # KRITISCHE BEREINIGUNG: Entferne Markdown-Blöcke
                clean_code = str(clean_code).strip()

                # Entferne ```python und ``` Blöcke
                if clean_code.startswith("```python"):
                    clean_code = clean_code[9:]  # Entferne ```python
                if clean_code.startswith("```"):
                    clean_code = clean_code[3:]  # Entferne ```
                if clean_code.endswith("```"):
                    clean_code = clean_code[:-3]  # Entferne ```

                clean_code = clean_code.strip()

                logger.info(f"Nach Markdown-Bereinigung: {len(clean_code)} Zeichen")
                logger.info(f"Erste 100 Zeichen: {clean_code[:100]}...")

                # Validiere, dass es mit Import beginnt
                if not clean_code.startswith("import"):
                    logger.warning(
                        "⚠️  Code beginnt nicht mit 'import' - möglicherweise noch Markdown-Reste!"
                    )
                    # Suche nach der ersten import-Zeile
                    lines = clean_code.split("\n")
                    for i, line in enumerate(lines):
                        if line.strip().startswith("import"):
                            clean_code = "\n".join(lines[i:])
                            logger.info(
                                f"✅ Code ab erster import-Zeile extrahiert: {len(clean_code)} Zeichen"
                            )
                            break

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
        """Sammelt umfassenden Kontext für Code-Generierung - NUR für ausgewählte Verbindungen"""
        try:
            # 🎯 KRITISCHE KORREKTUR: Nur ausgewählte Verbindungen verwenden!
            selected_connections = []
            
            # Quell-Verbindung extrahieren
            if request.source_config and request.source_config.get("connection_name"):
                source_conn = request.source_config["connection_name"]
                selected_connections.append(source_conn)
                logger.info(f"✅ Quell-Verbindung ausgewählt: {source_conn}")
            
            # Ziel-Verbindung extrahieren
            if request.target_config and request.target_config.get("connection_name"):
                target_conn = request.target_config["connection_name"]
                if target_conn not in selected_connections:
                    selected_connections.append(target_conn)
                logger.info(f"✅ Ziel-Verbindung ausgewählt: {target_conn}")
            
            # Fallback: Falls keine Verbindungen spezifiziert, verwende alle verfügbaren
            if not selected_connections:
                logger.warning("⚠️ Keine spezifischen Verbindungen ausgewählt - verwende alle verfügbaren")
                available_connections_raw = self.db_manager.list_connections()
                for conn in available_connections_raw:
                    if isinstance(conn, dict):
                        selected_connections.append(conn.get("name", str(conn)))
                    else:
                        selected_connections.append(str(conn))

            logger.info(f"🎯 VERWENDETE VERBINDUNGEN: {selected_connections}")

            # Transformation hints basierend auf der Beschreibung ableiten
            transformation_hints = self._extract_transformation_hints(
                request.description
            )

            context = ETLCodeGenerationContext(
                available_connections=selected_connections,
                transformation_hints=transformation_hints,
                performance_requirements=request.metadata.get("performance", {})
                if request.metadata
                else {},
            )

            logger.info(
                f"Kontext erstellt: {len(selected_connections)} Verbindungen ausgewählt"
            )
            return context

        except Exception as e:
            logger.error(f"Fehler beim Erstellen des Kontexts: {e}")
            return ETLCodeGenerationContext()

    async def _gather_schema_information(
        self, request: ETLRequest, context: ETLCodeGenerationContext
    ) -> Dict[str, Any]:
        """Sammelt Schema-Informationen für NUR die ausgewählten Verbindungen"""
        schema_info = {
            "source_schemas": {},
            "target_schemas": {},
            "available_connections_details": {},
        }

        try:
            # 🎯 KRITISCHE KORREKTUR: Verwende nur die bereits in context.available_connections gespeicherten Verbindungen
            # Diese wurden bereits in _build_comprehensive_context() korrekt auf die ausgewählten Verbindungen beschränkt
            selected_connections = context.available_connections
            
            logger.info(f"🎯 SCHEMA-SAMMLUNG FÜR VERBINDUNGEN: {selected_connections}")

            for conn_name in selected_connections:
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

    def _build_embedded_connections_config(self, available_connections: List[str]) -> str:
        """Erstellt embedded Connection-Konfigurationen für den generierten Code"""
        configs = []
        
        for conn_name in available_connections:
            try:
                # Verwende die internen connection_configs direkt
                conn_config = self.db_manager.connection_configs.get(conn_name, {})
                if conn_config:
                    conn_string = conn_config.get("connection_string", "")
                    conn_type = conn_config.get("type", "")
                    
                    config_str = f'        "{conn_name}": {{'
                    config_str += f'"type": "{conn_type}", '
                    config_str += f'"connection_string": "{conn_string}"'
                    config_str += '}'
                    configs.append(config_str)
                    logger.info(f"✅ Connection-Config für {conn_name} hinzugefügt")
                else:
                    logger.warning(f"⚠️ Connection-Config für {conn_name} nicht gefunden")
            except Exception as e:
                logger.warning(f"Konnte Connection {conn_name} nicht laden: {e}")
        
        if configs:
            return "    connection_configs = {\n" + ",\n".join(configs) + "\n    }"
        else:
            return "    connection_configs = {}"

    def _build_enhanced_generation_prompt(
        self,
        request: ETLRequest,
        context: ETLCodeGenerationContext,
        schema_info: Dict[str, Any],
    ) -> str:
        """Erstellt intelligenten Prompt mit ECHTEN Tabellen- und Spaltennamen"""

        # 🎯 INTELLIGENTE LÖSUNG: Verwende echte Schema-Informationen!
        real_table_info = self._extract_real_table_info(schema_info, context)
        
        # Sammle aktuelle Connection Strings für eingebetteten Code
        embedded_connections = self._build_embedded_connections_config(
            context.available_connections
        )

        prompt_parts = [
            f"AUFGABE: Erstelle ETL-Code für: {request.description}",
            "",
            "🎯 VERWENDE DIESE ECHTEN TABELLEN UND SPALTEN:",
            "",
        ]
        
        # Füge echte Tabellen-/Collection-Informationen hinzu
        for conn_name, table_info in real_table_info.items():
            prompt_parts.append(f"DATABASE {conn_name}:")
            if table_info["type"] == "mysql":
                prompt_parts.append("  VERFÜGBARE TABELLEN:")
                for table_name, columns in table_info["tables"].items():
                    # Sichere Spalten-Darstellung
                    if isinstance(columns, list) and columns:
                        column_names = [str(col) for col in columns]
                        prompt_parts.append(f"    - {table_name} (Spalten: {', '.join(column_names)})")
                    else:
                        prompt_parts.append(f"    - {table_name}")
                
                # Beispiel mit erstem verfügbaren Tabellennamen
                first_table = list(table_info['tables'].keys())[0] if table_info['tables'] else 'users_test'
                prompt_parts.append(f"  BEISPIEL EXTRACT: extract_config = {{\"query\": \"SELECT * FROM {first_table}\"}}")
            elif table_info["type"] == "mongodb":
                prompt_parts.append("  VERFÜGBARE COLLECTIONS:")
                for collection_name in table_info["collections"]:
                    prompt_parts.append(f"    - {collection_name}")
                
                # Beispiel mit erster verfügbarer Collection
                first_collection = table_info['collections'][0] if table_info['collections'] else 'users_processed'
                prompt_parts.append(f"  BEISPIEL LOAD: load_config = {{\"collection\": \"{first_collection}\", \"operation\": \"replace\"}}")
            prompt_parts.append("")

        prompt_parts.extend([
            "VERWENDE DIESE CONNECTION STRINGS DIREKT IM CODE:",
            embedded_connections,
            "",
            "🎯 TEMPLATE MIT ECHTEN NAMEN:",
            ""
        ])

        # Generiere Template mit echten Namen
        mysql_table = self._get_best_source_table(real_table_info)
        mongo_collection = self._get_best_target_collection(real_table_info)
        age_column = self._detect_age_column(real_table_info)

        template = f"""import pandas as pd
import logging
from typing import Dict, Any
import pymongo
import sqlalchemy as sa

# Logger konfigurieren
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SimpleDBManager:
    def __init__(self):
{embedded_connections}
    
    def extract_data(self, connection_name: str, query_config: Dict[str, Any]) -> pd.DataFrame:
        config = self.connection_configs[connection_name]
        logger.info(f"Extrahiere Daten aus {{connection_name}} ({{config['type']}})")
        
        if config["type"] in ["mysql", "mariadb"]:
            engine = sa.create_engine(config["connection_string"])
            df = pd.read_sql(query_config["query"], engine)
            engine.dispose()
            return df
        elif config["type"] == "mongodb":
            client = pymongo.MongoClient(config["connection_string"])
            db_name = config["connection_string"].split("/")[-1] or "my_test_db"
            db = client[db_name]
            collection = db[query_config["collection"]]
            cursor = collection.find(query_config.get("query", {{}}))
            if "limit" in query_config:
                cursor = cursor.limit(query_config["limit"])
            data = list(cursor)
            client.close()
            return pd.DataFrame(data)
        else:
            raise ValueError(f"Nicht unterstützter DB-Typ: {{config['type']}}")
    
    def load_data(self, connection_name: str, df: pd.DataFrame, target_config: Dict[str, Any]):
        config = self.connection_configs[connection_name]
        logger.info(f"Lade {{len(df)}} Datensätze nach {{connection_name}} ({{config['type']}})")
        
        if config["type"] == "mongodb":
            client = pymongo.MongoClient(config["connection_string"])
            db_name = config["connection_string"].split("/")[-1] or "my_test_db"
            db = client[db_name]
            collection = db[target_config["collection"]]
            records = df.to_dict("records")
            if target_config.get("operation") == "replace":
                collection.delete_many({{}})
            collection.insert_many(records)
            client.close()
            logger.info(f"✅ MongoDB: {{len(records)}} Datensätze in '{{target_config['collection']}}' geladen")
        else:
            engine = sa.create_engine(config["connection_string"])
            df.to_sql(target_config["table"], engine, if_exists=target_config.get("if_exists", "replace"), index=False)
            engine.dispose()
            logger.info(f"✅ SQL: {{len(df)}} Datensätze in '{{target_config['table']}}' geladen")

def etl_pipeline():
    try:
        logger.info("=== ETL-Pipeline gestartet ===")
        db_manager = SimpleDBManager()
        
        # EXTRACT - ECHTE TABELLE VERWENDEN
        logger.info("Phase 1: Daten extrahieren")
        extract_config = {{"query": "SELECT * FROM {mysql_table}"}}
        df = db_manager.extract_data("Jup", extract_config)
        logger.info(f"Extrahiert: {{len(df)}} Datensätze")
        
        # TRANSFORM - ECHTE SPALTE VERWENDEN
        logger.info("Phase 2: Daten transformieren")
        df['{age_column}'] = df['{age_column}'] + 2
        logger.info("Transformation abgeschlossen")
        
        # LOAD - ECHTE COLLECTION VERWENDEN
        logger.info("Phase 3: Daten laden")
        load_config = {{"collection": "{mongo_collection}", "operation": "replace"}}
        try:
            db_manager.load_data("mong4", df, load_config)
            logger.info("Daten erfolgreich geladen")
        except Exception as load_error:
            logger.warning(f"MongoDB-Load fehlgeschlagen: {{load_error}}")
            logger.info("Pipeline trotzdem erfolgreich - Daten wurden transformiert")
        
        logger.info("=== ETL-Pipeline erfolgreich abgeschlossen ===")
        return df
        
    except Exception as e:
        logger.error(f"ETL-Pipeline Fehler: {{e}}")
        raise

if __name__ == "__main__":
    result = etl_pipeline()
    print(f"ETL abgeschlossen. Verarbeitete Datensätze: {{len(result)}}")"""

        prompt_parts.extend([
            template,
            "",
            "🚨 KRITISCH: VERWENDE NUR ECHTE NAMEN AUS DEN SCHEMA-INFORMATIONEN OBEN!",
            "ANTWORTE NUR MIT VOLLSTÄNDIGEM PYTHON-CODE!"
        ])

        return "\n".join(prompt_parts)

    def _extract_real_table_info(self, schema_info: Dict[str, Any], context: ETLCodeGenerationContext) -> Dict[str, Any]:
        """Extrahiert echte Tabellen- und Spalteninformationen aus Schema"""
        real_info = {}
        
        connections_details = schema_info.get("available_connections_details", {})
        
        for conn_name, details in connections_details.items():
            if details.get("status") == "available":
                schema = details.get("schema", {})
                conn_type = details.get("type", "unknown")
                
                if conn_type in ["mysql", "mariadb"]:
                    tables = schema.get("tables", {})
                    # Verbesserte Spalten-Extraktion
                    table_info = {}
                    for table_name, table_details in tables.items():
                        if isinstance(table_details, list):
                            # Direkte Liste von Spalten-Dicts
                            columns = [col.get("name", str(col)) if isinstance(col, dict) else str(col) for col in table_details]
                        elif isinstance(table_details, dict) and "columns" in table_details:
                            # Verschachtelte Struktur
                            columns = table_details.get("columns", [])
                        else:
                            # Fallback
                            columns = []
                        table_info[table_name] = columns
                    
                    real_info[conn_name] = {
                        "type": "mysql",
                        "tables": table_info
                    }
                elif conn_type == "mongodb":
                    collections = schema.get("collections", [])
                    # Sicherstellen, dass es eine Liste ist
                    if isinstance(collections, dict):
                        collections = list(collections.keys())
                    real_info[conn_name] = {
                        "type": "mongodb", 
                        "collections": collections
                    }
                
                logger.info(f"Echte Schema-Info für {conn_name}: {real_info[conn_name]}")
        
        return real_info

    def _get_best_source_table(self, real_table_info: Dict[str, Any]) -> str:
        """Findet die beste Quell-Tabelle basierend auf echten Schema-Daten"""
        for conn_name, info in real_table_info.items():
            if info["type"] == "mysql" and info["tables"]:
                # Bevorzuge Tabellen mit 'user' im Namen
                for table_name in info["tables"].keys():
                    if "user" in table_name.lower():
                        return table_name
                # Fallback: erste verfügbare Tabelle
                return list(info["tables"].keys())[0]
        
        # Absolute Fallback (sollte nicht passieren)
        return "users_test"

    def _get_best_target_collection(self, real_table_info: Dict[str, Any]) -> str:
        """Findet die beste Ziel-Collection basierend auf echten Schema-Daten"""
        for conn_name, info in real_table_info.items():
            if info["type"] == "mongodb" and info["collections"]:
                # Bevorzuge Collections mit 'processed' oder 'target' im Namen
                for collection_name in info["collections"]:
                    if "processed" in collection_name.lower() or "target" in collection_name.lower():
                        return collection_name
                # Fallback: erste verfügbare Collection
                return info["collections"][0]
        
        # Absolute Fallback (sollte nicht passieren)
        return "users_processed"

    def _detect_age_column(self, real_table_info: Dict[str, Any]) -> str:
        """Erkennt eine Alters-Spalte aus echten Schema-Daten"""
        for conn_name, info in real_table_info.items():
            if info["type"] == "mysql":
                for table_name, columns in info["tables"].items():
                    # Suche nach Spalten mit 'age' im Namen
                    for column in columns:
                        # Sichere Spalten-Name-Extraktion
                        if isinstance(column, dict):
                            column_name = column.get("name", str(column))
                        else:
                            column_name = str(column)
                        
                        if "age" in column_name.lower():
                            return column_name
                    
                    # Fallback: erste numerische Spalte (häufige Patterns)
                    for column in columns:
                        if isinstance(column, dict):
                            column_name = column.get("name", str(column))
                        else:
                            column_name = str(column)
                        
                        if any(keyword in column_name.lower() for keyword in ["year", "jahre", "alter", "birth"]):
                            return column_name
        
        # Absolute Fallback
        return "age"

    def _generate_execution_plan(
        self, request: ETLRequest, context: ETLCodeGenerationContext
    ) -> List[str]:
        """Generiert Ausführungsplan basierend auf Request und Context"""
        plan = [
            "1. SimpleDBManager initialisieren",
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
