"""
Database Manager für Multi-Database Support
Basiert auf dem Dataanalyzer-Ansatz mit persistenter Speicherung
"""

import logging
import json
import os
from typing import Dict, List, Any, Optional, Union
from enum import Enum
import pandas as pd
from urllib.parse import urlparse
from pathlib import Path

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
    """Multi-Database Manager für ETL-Operationen mit persistenter Speicherung"""

    def __init__(self, config_file: str = "db_connections.json"):
        self.connections: Dict[str, Any] = {}
        self.connection_configs: Dict[str, Dict] = {}
        self.config_file = config_file
        
        # Lade gespeicherte Verbindungen beim Start
        self._load_connections()

    def _load_connections(self):
        """Lädt gespeicherte Verbindungen aus JSON-Datei"""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    saved_configs = json.load(f)
                    logger.info(f"Lade {len(saved_configs)} gespeicherte Verbindungen aus {self.config_file}")
                    
                    for name, config in saved_configs.items():
                        try:
                            # Konfiguration laden, aber Verbindung erst bei Bedarf herstellen
                            self.connection_configs[name] = config
                            logger.info(f"Verbindung '{name}' aus Datei geladen ({config.get('type', 'unknown')})")
                        except Exception as e:
                            logger.error(f"Fehler beim Laden der Verbindung '{name}': {e}")
            else:
                logger.info(f"Keine gespeicherte Konfigurationsdatei gefunden: {self.config_file}")
        except Exception as e:
            logger.error(f"Fehler beim Laden der Verbindungen: {e}")

    def _save_connections(self):
        """Speichert aktuelle Verbindungen in JSON-Datei"""
        try:
            # Nur die Konfigurationen speichern, nicht die aktiven Verbindungen
            configs_to_save = {}
            for name, config in self.connection_configs.items():
                # Sensible Daten nicht im Klartext speichern (für Produktionsumgebung)
                configs_to_save[name] = config.copy()
                
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(configs_to_save, f, indent=2, ensure_ascii=False)
                
            logger.info(f"Verbindungen in {self.config_file} gespeichert ({len(configs_to_save)} Verbindungen)")
        except Exception as e:
            logger.error(f"Fehler beim Speichern der Verbindungen: {e}")

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

            # Verbindungen persistent speichern
            self._save_connections()
            
            logger.info(
                f"Datenbankverbindung '{name}' ({db_type.value}) erfolgreich hinzugefügt und gespeichert"
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
        """Gibt Datenbankverbindung zurück - mit lazy loading"""
        if name not in self.connection_configs:
            raise ValueError(f"Verbindung '{name}' nicht konfiguriert")
            
        # Lazy loading: Verbindung erst bei Bedarf herstellen
        if name not in self.connections:
            try:
                config = self.connection_configs[name]
                db_type = self._detect_database_type(config)
                connector = self._create_connector(db_type, config)
                self.connections[name] = connector
                logger.info(f"Lazy-loaded Verbindung '{name}' ({db_type.value})")
            except Exception as e:
                logger.error(f"Fehler beim Lazy-Loading der Verbindung '{name}': {e}")
                raise
                
        return self.connections[name]

    def remove_connection(self, name: str) -> bool:
        """Entfernt eine Datenbankverbindung"""
        try:
            if name in self.connections:
                # Verbindung schließen falls möglich
                if hasattr(self.connections[name], 'close'):
                    self.connections[name].close()
                del self.connections[name]
                
            if name in self.connection_configs:
                del self.connection_configs[name]
                self._save_connections()
                logger.info(f"Verbindung '{name}' entfernt")
                return True
            else:
                logger.warning(f"Verbindung '{name}' nicht gefunden")
                return False
        except Exception as e:
            logger.error(f"Fehler beim Entfernen der Verbindung '{name}': {e}")
            return False

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

    def test_connection(
        self, connection_name: str, timeout: int = 5
    ) -> Dict[str, Any]:
        """Testet Datenbankverbindung mit robustem Thread-Timeout"""
        import time
        import threading

        def test_worker():
            """Worker für Connection Test - macht echten DB-Test"""
            start_time = time.time()
            try:
                connector = self.get_connection(connection_name)
                db_type = self.connection_configs[connection_name]["type"]
                
                # Echte Verbindung und einfache Abfrage
                if db_type == "mongodb":
                    # MongoDB: Ping-Test
                    connector.client.admin.command('ping')
                    databases = connector.list_databases()
                    elapsed = round(time.time() - start_time, 2)
                    return {
                        "status": "success",
                        "message": f"✅ MongoDB verbunden ({elapsed}s). {len(databases)} Datenbanken gefunden.",
                        "details": {"databases": databases[:5], "elapsed_time": elapsed, "type": db_type}
                    }
                else:
                    # SQL: Simple SELECT 1 Test
                    connector.execute_query("SELECT 1")
                    tables = connector.get_table_names()
                    elapsed = round(time.time() - start_time, 2)
                    return {
                        "status": "success",
                        "message": f"✅ {db_type.upper()}-Datenbank verbunden ({elapsed}s). {len(tables)} Tabellen gefunden.",
                        "details": {"tables": tables[:10], "elapsed_time": elapsed, "type": db_type}
                    }
                    
            except Exception as e:
                elapsed = round(time.time() - start_time, 2)
                return {
                    "status": "error", 
                    "message": f"❌ Verbindungsfehler ({elapsed}s): {str(e)}",
                    "details": {"connection_name": connection_name, "error": str(e), "elapsed_time": elapsed}
                }

        # Thread-basierter Timeout
        result = [None]
        
        def target():
            result[0] = test_worker()
        
        thread = threading.Thread(target=target)
        thread.daemon = True
        thread.start()
        thread.join(timeout=timeout)
        
        if thread.is_alive():
            # Thread läuft noch - Timeout erreicht
            return {
                "status": "error",
                "message": f"❌ Verbindungstest nach {timeout} Sekunden abgebrochen (Timeout)",
                "details": {"connection_name": connection_name, "timeout": timeout}
            }
        
        # Thread beendet - Ergebnis zurückgeben
        return result[0] or {
            "status": "error",
            "message": "❌ Unbekannter Fehler beim Verbindungstest",
            "details": {"connection_name": connection_name}
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

    def get_schema_info(self, connection_name: str, database: str = None) -> Dict[str, Any]:
        """Fragt Schema-Informationen ab: Tabellen, Spalten, Typen"""
        connector = self.get_connection(connection_name)
        db_type = self.connection_configs[connection_name]["type"]
        
        try:
            if db_type == "mongodb":
                # MongoDB: Collections und Sample-Dokumente
                if not database:
                    raise ValueError("Database name required for MongoDB")
                    
                collections = connector.list_collections(database)
                schema_info = {
                    "type": "mongodb",
                    "database": database,
                    "collections": {}
                }
                
                for collection in collections:
                    # Sample-Dokument für Schema-Erkennung
                    sample = connector.execute_raw_query(
                        database, collection, "find_one", {}, {}
                    )
                    if sample:
                        schema_info["collections"][collection] = {
                            "sample_fields": list(sample.keys()),
                            "sample_document": sample
                        }
                    else:
                        schema_info["collections"][collection] = {
                            "sample_fields": [],
                            "sample_document": None
                        }
                        
                return schema_info
                
            else:
                # SQL-Datenbanken: Tabellen und Spalten
                if db_type == "postgresql":
                    # PostgreSQL Information Schema
                    query = """
                    SELECT 
                        table_name,
                        column_name,
                        data_type,
                        is_nullable,
                        column_default
                    FROM information_schema.columns 
                    WHERE table_schema = 'public'
                    ORDER BY table_name, ordinal_position
                    """
                elif db_type in ["mysql", "mariadb"]:
                    # MySQL Information Schema
                    query = """
                    SELECT 
                        table_name,
                        column_name,
                        data_type,
                        is_nullable,
                        column_default
                    FROM information_schema.columns 
                    WHERE table_schema = DATABASE()
                    ORDER BY table_name, ordinal_position
                    """
                elif db_type == "sqlite":
                    # SQLite: Liste alle Tabellen
                    query = """
                    SELECT name as table_name FROM sqlite_master 
                    WHERE type='table' AND name NOT LIKE 'sqlite_%'
                    """
                else:
                    # Fallback für andere SQL-DBs
                    query = """
                    SELECT table_name, column_name, data_type 
                    FROM information_schema.columns 
                    ORDER BY table_name
                    """
                
                result_df = connector.execute_query(query)
                
                # Strukturiere als Schema-Dictionary
                schema_info = {
                    "type": db_type,
                    "tables": {}
                }
                
                if db_type == "sqlite":
                    # Für SQLite: Hole Spalten für jede Tabelle einzeln
                    for _, row in result_df.iterrows():
                        table_name = row["table_name"]
                        pragma_query = f"PRAGMA table_info({table_name})"
                        columns_df = connector.execute_query(pragma_query)
                        
                        schema_info["tables"][table_name] = {
                            "columns": []
                        }
                        
                        for _, col_row in columns_df.iterrows():
                            schema_info["tables"][table_name]["columns"].append({
                                "name": col_row["name"],
                                "type": col_row["type"],
                                "nullable": not bool(col_row["notnull"]),
                                "default": col_row["dflt_value"]
                            })
                else:
                    # Für andere SQL-DBs: Gruppiere nach Tabelle
                    for _, row in result_df.iterrows():
                        table_name = row["table_name"]
                        if table_name not in schema_info["tables"]:
                            schema_info["tables"][table_name] = {"columns": []}
                        
                        schema_info["tables"][table_name]["columns"].append({
                            "name": row["column_name"],
                            "type": row["data_type"],
                            "nullable": row.get("is_nullable", "YES") == "YES",
                            "default": row.get("column_default")
                        })
                
                return schema_info
                
        except Exception as e:
            logger.error(f"Schema-Introspection für {connection_name} fehlgeschlagen: {e}")
            return {
                "type": db_type,
                "error": str(e),
                "tables": {} if db_type != "mongodb" else {},
                "collections": {} if db_type == "mongodb" else {}
            }
