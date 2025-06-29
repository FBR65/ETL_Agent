"""
MongoDB Connector für ETL-Agent
Übernommen aus dataanalyser
"""

import logging
import time
from typing import Any, List, Dict, Optional, Union
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure, ConfigurationError
from bson import ObjectId
from pprint import pprint

try:
    import openai
except ImportError:
    openai = None

logger = logging.getLogger("etl_agent.mongodb_connector")


class MongoDBConnector:
    """
    Stellt Operationen für MongoDB bereit, einschließlich Auflisten von Datenbanken,
    Collections, Collection-Strukturen und Generieren von MongoDB-Abfragen
    aus natürlicher Sprache.
    """

    def __init__(
        self,
        connection_string: str = "mongodb://localhost:27017/",
        openai_api_key: Optional[str] = None,
        openai_api_base: Optional[str] = None,
        llm_model_name: str = "qwen2.5",
        auth_source: Optional[str] = "admin",
    ):
        """Initialize MongoDB connector with improved authentication handling."""
        self.connection_string = connection_string
        self.client: Optional[MongoClient] = None
        self.openai_api_key = openai_api_key
        self.openai_api_base = openai_api_base
        self.llm_model_name = llm_model_name
        self.auth_source = auth_source
        self.logger = logging.getLogger(__name__)

        # ...existing code... (komplette Initialisierung aus kopiertem Code)

        try:
            from urllib.parse import urlparse

            parsed_uri = urlparse(connection_string)

            if not parsed_uri.username and not parsed_uri.password:
                self.client = MongoClient(
                    connection_string,
                    serverSelectionTimeoutMS=5000,
                    connectTimeoutMS=5000,
                )
            else:
                self.client = MongoClient(
                    connection_string,
                    serverSelectionTimeoutMS=5000,
                    connectTimeoutMS=5000,
                    authSource=self.auth_source,
                )

            self.client.admin.command("ping")
            logger.info(f"MongoDB client successfully initialized")
        except Exception as e:
            logger.error(f"MongoDB connection error: {e}")
            self.client = None
            raise

    def _mask_uri_credentials(self, uri: str) -> str:
        # ...existing code...
        try:
            from urllib.parse import urlparse, urlunparse

            parsed = urlparse(uri)
            if (parsed.username or parsed.password) and parsed.hostname:
                masked_netloc = f"****:****@{parsed.hostname}"
                if parsed.port:
                    masked_netloc += f":{parsed.port}"
                parsed = parsed._replace(netloc=masked_netloc)
                return urlunparse(parsed)
            return uri
        except Exception:
            return "mongodb://<credentials_masked>@<host>"

    def close_connection(self):
        # ...existing code...
        if self.client:
            self.client.close()
            logger.info("MongoDB-Verbindung geschlossen.")

    def list_databases(self) -> List[str]:
        # ...existing code...
        if not self.client:
            logger.error("MongoDB-Client nicht initialisiert.")
            raise RuntimeError("MongoDB-Client nicht initialisiert.")
        try:
            db_names = self.client.list_database_names()
            logger.debug(f"Verfügbare Datenbanken: {db_names}")
            return db_names
        except OperationFailure as e:
            logger.error(f"Fehler beim Auflisten der Datenbanken: {e}")
            raise
        except Exception as e:
            logger.error(f"Unerwarteter Fehler beim Auflisten der Datenbanken: {e}")
            raise

    def list_collections(self, db_name: str) -> List[str]:
        # ...existing code...
        if not self.client:
            logger.error("MongoDB client is not initialized.")
            raise RuntimeError("MongoDB client is not initialized.")
        if not db_name:
            raise ValueError("Database name cannot be empty.")

        try:
            if db_name not in self.list_databases():
                raise ValueError(f"Database '{db_name}' does not exist.")

            db = self.client[db_name]
            collection_names = db.list_collection_names()
            logger.debug(f"Collections in database '{db_name}': {collection_names}")
            return collection_names
        except Exception as e:
            logger.error(f"Error listing collections for database '{db_name}': {e}")
            raise

    def get_collection_structure(
        self, db_name: str, collection_name: str, sample_size: int = 1
    ) -> List[Dict[str, Any]]:
        # ...existing code... (komplette Implementierung)
        if not self.client:
            logger.error("MongoDB-Client nicht initialisiert.")
            raise RuntimeError("MongoDB-Client nicht initialisiert.")

        structure_map: Dict[str, str] = {}
        field_order: List[str] = []

        try:
            db = self.client[db_name]
            collection = db[collection_name]
            documents_to_inspect = list(collection.find().limit(sample_size))

            if not documents_to_inspect:
                logger.info(f"Collection '{db_name}.{collection_name}' ist leer")
                return []

            for doc in documents_to_inspect:
                if not doc:
                    continue
                for key, value in doc.items():
                    if key not in structure_map:
                        field_order.append(key)
                        structure_map[key] = type(value).__name__

            schema_representation = []
            for field_name in field_order:
                schema_representation.append(
                    {"name": field_name, "type": structure_map[field_name]}
                )

            return schema_representation

        except Exception as e:
            logger.error(f"Fehler beim Ermitteln der Struktur: {e}")
            raise

    def execute_raw_query(
        self,
        db_name: str,
        collection_name: str,
        query_type: str,
        query_details: Union[Dict, List],
        find_options: Optional[Dict] = None,
    ) -> List[Dict[str, Any]]:
        # ...existing code...
        if not self.client:
            raise RuntimeError("MongoDB-Client nicht initialisiert.")

        db = self.client[db_name]
        collection = db[collection_name]
        results = []

        try:
            if query_type == "find":
                cursor = collection.find(query_details, **(find_options or {}))
                results = list(cursor)
            elif query_type == "aggregate":
                cursor = collection.aggregate(query_details)
                results = list(cursor)
            else:
                raise ValueError(f"Nicht unterstützter query_type: {query_type}")

            def convert_objectids_in_doc(doc):
                if isinstance(doc, list):
                    return [convert_objectids_in_doc(item) for item in doc]
                if isinstance(doc, dict):
                    return {
                        key: convert_objectids_in_doc(value)
                        for key, value in doc.items()
                    }
                if isinstance(doc, ObjectId):
                    return str(doc)
                return doc

            return [convert_objectids_in_doc(doc.copy()) for doc in results]

        except Exception as e:
            logger.error(f"Fehler während der Rohabfrageausführung: {e}")
            raise

    def get_collection(self, db_name: str, collection_name: str):
        # ...existing code...
        if not self.client:
            raise RuntimeError("MongoDB client is not initialized.")
        db = self.client[db_name]
        return db[collection_name]
