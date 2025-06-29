"""
Database Manager für Multi-Database Support
Basiert auf dem Dataanalyzer-Ansatz
"""

import logging
from typing import Dict, List, Any, Optional, Union
from enum import Enum
import pandas as pd
from urllib.parse import urlparse

# Database Connectors
from .connectors.mongodb_connector import MongoDBConnector
from .connectors.sql_connector import SQLConnector

# Oracle Connector optional
try:
    from .connectors.oracle_connector import OracleConnector
except ImportError:
    OracleConnector = None

logger = logging.getLogger(__name__)


class DatabaseType(Enum):
    """Unterstützte Datenbanktypen"""

    MONGODB = "mongodb"
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    MARIADB = "mariadb"
    ORACLE = "oracle"
    SQLITE = "sqlite"
    SQLSERVER = "sqlserver"


class DatabaseManager:
    """Multi-Database Manager für ETL-Operationen"""

    def __init__(self):
        self.connections: Dict[str, Any] = {}
        self.connection_configs: Dict[str, Dict] = {}

    def add_connection(self, name: str, connection_config: Dict[str, Any]) -> bool:
        """Fügt eine neue Datenbankverbindung hinzu"""
        try:
            logger.info(
                f"Versuche Verbindung '{name}' hinzuzufügen mit config: {connection_config}"
            )

            db_type = self._detect_database_type(connection_config)
            logger.info(f"Erkannter Datenbanktyp für '{name}': {db_type}")

            connector = self._create_connector(db_type, connection_config)
            logger.info(
                f"Connector für '{name}' erfolgreich erstellt: {type(connector)}"
            )

            self.connections[name] = connector
            self.connection_configs[name] = {**connection_config, "type": db_type.value}

            logger.info(
                f"Datenbankverbindung '{name}' ({db_type.value}) erfolgreich hinzugefügt"
            )
            return True

        except Exception as e:
            logger.error(
                f"Fehler beim Hinzufügen der Verbindung '{name}': {e}", exc_info=True
            )
            return False

    def _detect_database_type(self, config: Dict[str, Any]) -> DatabaseType:
        """Erkennt Datenbanktyp aus Konfiguration"""
        if "connection_string" in config:
            conn_str = config["connection_string"].lower()

            if conn_str.startswith("mongodb://") or conn_str.startswith(
                "mongodb+srv://"
            ):
                return DatabaseType.MONGODB
            elif "postgresql://" in conn_str or "postgres://" in conn_str:
                return DatabaseType.POSTGRESQL
            elif (
                "mysql://" in conn_str or "mariadb://" in conn_str
            ):  # MariaDB auch als MySQL behandeln
                return DatabaseType.MYSQL
            elif "oracle://" in conn_str:
                return DatabaseType.ORACLE
            elif "sqlite://" in conn_str:
                return DatabaseType.SQLITE
            elif "mssql://" in conn_str or "sqlserver://" in conn_str:
                return DatabaseType.SQLSERVER

        # Fallback: expliziter Typ
        db_type = config.get("type", "").lower()
        if db_type:
            # MariaDB als MySQL-Alias behandeln
            if db_type == "mariadb":
                return DatabaseType.MYSQL

            for dtype in DatabaseType:
                if dtype.value == db_type:
                    return dtype

        raise ValueError(f"Unbekannter Datenbanktyp: {config}")

    def _create_connector(self, db_type: DatabaseType, config: Dict[str, Any]):
        """Erstellt passenden Connector für Datenbanktyp"""
        if db_type == DatabaseType.MONGODB:
            return MongoDBConnector(
                connection_string=config["connection_string"],
                **config.get("mongodb_options", {}),
            )
        elif db_type in [
            DatabaseType.POSTGRESQL,
            DatabaseType.MYSQL,
            DatabaseType.MARIADB,
            DatabaseType.SQLITE,
            DatabaseType.SQLSERVER,
        ]:
            # MariaDB als MySQL an SQLConnector übergeben
            connector_db_type = (
                "mysql" if db_type == DatabaseType.MARIADB else db_type.value
            )
            return SQLConnector(
                connection_string=config["connection_string"], db_type=connector_db_type
            )
        elif db_type == DatabaseType.ORACLE:
            if OracleConnector is None:
                raise ImportError("Oracle-Connector nicht verfügbar")
            return OracleConnector(
                connection_string=config["connection_string"],
                **config.get("oracle_options", {}),
            )
        else:
            raise ValueError(f"Nicht unterstützter Datenbanktyp: {db_type}")

    def get_connection(self, name: str):
        """Gibt Datenbankverbindung zurück"""
        if name not in self.connections:
            raise ValueError(f"Verbindung '{name}' nicht gefunden")
        return self.connections[name]

    def list_connections(self) -> List[Dict[str, Any]]:
        """Listet alle verfügbaren Verbindungen auf"""
        return [
            {
                "name": name,
                "type": config.get("type"),
                "status": "connected" if name in self.connections else "disconnected",
            }
            for name, config in self.connection_configs.items()
        ]

    def get_database_schema(self, connection_name: str) -> Dict[str, Any]:
        """Gibt Datenbankschema für Verbindung zurück"""
        try:
            connector = self.get_connection(connection_name)
            db_type = self.connection_configs[connection_name]["type"]

            if db_type == "mongodb":
                databases = connector.list_databases()
                schema = {}
                for db_name in databases[:3]:  # Limitiert für Performance
                    collections = connector.list_collections(db_name)
                    schema[db_name] = {
                        "collections": collections,
                        "sample_structures": {},
                    }
                    # Sample-Struktur für erste Collection
                    if collections:
                        structure = connector.get_collection_structure(
                            db_name, collections[0]
                        )
                        schema[db_name]["sample_structures"][collections[0]] = structure
                return schema
            else:
                # SQL-Datenbanken
                tables = connector.get_table_names()
                schema = {"tables": tables, "schemas": {}}
                # Schema für erste paar Tabellen
                for table in tables[:5]:
                    schema["schemas"][table] = connector.get_table_schema(table)
                return schema
        except Exception as e:
            logger.error(f"Schema-Abruf Fehler für '{connection_name}': {e}")
            return {"error": str(e)}

    def extract_data(
        self, connection_name: str, query_config: Dict[str, Any]
    ) -> pd.DataFrame:
        """Extrahiert Daten aus Datenbank"""
        connector = self.get_connection(connection_name)
        db_type = self.connection_configs[connection_name]["type"]

        if db_type == "mongodb":
            # MongoDB-Abfrage
            db_name = query_config["database"]
            collection_name = query_config["collection"]
            query = query_config.get("query", {})
            limit = query_config.get("limit", 1000)

            results = connector.execute_raw_query(
                db_name, collection_name, "find", query, {"limit": limit}
            )
            return pd.DataFrame(results)
        else:
            # SQL-Abfrage
            query = query_config["query"]
            params = query_config.get("params")
            return connector.execute_query(query, params)

    def load_data(
        self, connection_name: str, df: pd.DataFrame, target_config: Dict[str, Any]
    ):
        """Lädt Daten in Zieldatenbank"""
        connector = self.get_connection(connection_name)
        db_type = self.connection_configs[connection_name]["type"]

        if db_type == "mongodb":
            # MongoDB-Insert
            db_name = target_config["database"]
            collection_name = target_config["collection"]

            collection = connector.get_collection(db_name, collection_name)
            records = df.to_dict("records")
            collection.insert_many(records)

            logger.info(f"{len(records)} Datensätze in MongoDB geladen")
        else:
            # SQL-Insert
            table_name = target_config["table"]
            if_exists = target_config.get("if_exists", "append")

            connector.insert_dataframe(df, table_name, if_exists)
            logger.info(f"DataFrame in SQL-Tabelle {table_name} geladen")

    def test_connection(self, connection_name: str) -> Dict[str, Any]:
        """Testet Datenbankverbindung"""
        try:
            connector = self.get_connection(connection_name)
            db_type = self.connection_configs[connection_name]["type"]

            if db_type == "mongodb":
                databases = connector.list_databases()
                return {
                    "status": "success",
                    "message": f"MongoDB verbunden. {len(databases)} Datenbanken gefunden.",
                    "details": {"databases": databases[:5]},
                }
            else:
                tables = connector.get_table_names()
                return {
                    "status": "success",
                    "message": f"SQL-Datenbank verbunden. {len(tables)} Tabellen gefunden.",
                    "details": {"tables": tables[:10]},
                }

        except Exception as e:
            return {
                "status": "error",
                "message": f"Verbindungstest fehlgeschlagen: {str(e)}",
            }

    def close_all_connections(self):
        """Schließt alle Datenbankverbindungen"""
        for name, connector in self.connections.items():
            try:
                if hasattr(connector, "close"):
                    connector.close()
                elif hasattr(connector, "close_connection"):
                    connector.close_connection()
                logger.info(f"Verbindung '{name}' geschlossen")
            except Exception as e:
                logger.error(f"Fehler beim Schließen der Verbindung '{name}': {e}")

        self.connections.clear()
