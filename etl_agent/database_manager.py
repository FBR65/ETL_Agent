"""
Database Manager für Multi-Database Support
Basiert auf dem Dataanalyzer-Ansatz mit persistenter Speicherung
"""

import logging
import json
import os
import time
from typing import Dict, List, Any
from enum import Enum
import pandas as pd
import threading

# Database Connectors
from .connectors.mongodb_connector import MongoDBConnector
from .connectors.sql_connector import SQLConnector
from .utils.logger import ETLDesignerLogger

# Oracle Connector optional
try:
    from .connectors.oracle_connector import OracleConnector
except ImportError:
    OracleConnector = None

logger = logging.getLogger(__name__)
db_logger = ETLDesignerLogger("database_manager")


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
        self._lock = threading.RLock()  # Re-entrant Lock für robuste Thread-Sicherheit

        # Lade gespeicherte Verbindungen beim Start
        self._load_connections()

    def _load_connections(self):
        """Lädt gespeicherte Verbindungen aus JSON-Datei"""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, "r", encoding="utf-8") as f:
                    saved_configs = json.load(f)
                    logger.info(
                        f"Lade {len(saved_configs)} gespeicherte Verbindungen aus {self.config_file}"
                    )

                    # Flag um zu prüfen, ob Korrekturen gemacht wurden
                    corrections_made = False

                    with self._lock:
                        for name, config in saved_configs.items():
                            try:
                                # MySQL/MariaDB Connection String beim Laden korrigieren
                                if config.get("type", "").lower() in [
                                    "mysql",
                                    "mariadb",
                                ]:
                                    if "connection_string" in config:
                                        original_conn_str = config["connection_string"]
                                        corrected_conn_str = (
                                            self._fix_mysql_connection_string(
                                                original_conn_str
                                            )
                                        )
                                        if corrected_conn_str != original_conn_str:
                                            config["connection_string"] = (
                                                corrected_conn_str
                                            )
                                            corrections_made = True
                                            logger.info(
                                                f"Connection String für '{name}' beim Laden korrigiert"
                                            )

                                # Konfiguration laden, aber Verbindung erst bei Bedarf herstellen
                                self.connection_configs[name] = config
                                logger.info(
                                    f"Verbindung '{name}' aus Datei geladen ({config.get('type', 'unknown')})"
                                )
                            except Exception as e:
                                logger.error(
                                    f"Fehler beim Laden der Konfiguration für '{name}': {e}"
                                )

                    # Wenn Korrekturen gemacht wurden, die Datei aktualisieren
                    if corrections_made:
                        self._save_connections()
                        logger.info(
                            "JSON-Datei nach MySQL Connection String Korrekturen aktualisiert"
                        )
            else:
                logger.info(
                    f"Keine gespeicherte Konfigurationsdatei gefunden: {self.config_file}"
                )
        except Exception as e:
            logger.error(f"Fehler beim Laden der Verbindungen: {e}")

    def _save_connections(self):
        """
        Speichert die aktuellen Verbindungskonfigurationen thread-sicher in die JSON-Datei.
        Die langsame I/O-Operation wird außerhalb des Locks ausgeführt.
        """
        with self._lock:
            # Erstelle eine Kopie der Konfigurationen innerhalb des Locks
            configs_to_save = {
                name: {
                    "connection_string": conf.get("connection_string", ""),
                    "type": conf.get("type", ""),
                }
                for name, conf in self.connection_configs.items()
            }

        # Führe die langsame Datei-I/O außerhalb des Locks aus
        try:
            with open(self.config_file, "w", encoding="utf-8") as f:
                json.dump(configs_to_save, f, indent=2, ensure_ascii=False)
            logger.info(
                f"[OK] {len(configs_to_save)} Verbindungen in {self.config_file} gespeichert."
            )
        except Exception as e:
            logger.error(f"❌ Speichervorgang fehlgeschlagen: {e}")
            # Hier keinen Fehler auslösen, um die Anwendung nicht zum Absturz zu bringen

    def add_connection(self, name: str, connection_config: Dict[str, Any]) -> bool:
        """Fügt eine neue Datenbankverbindung hinzu - optimiert für Race Condition-Vermeidung"""
        try:
            # Enhanced Logging: Start
            db_logger.log_database_operation(
                "add_connection_started",
                name,
                success=True,
                details={"config_keys": list(connection_config.keys())},
            )

            logger.info(
                f"Versuche Verbindung '{name}' hinzuzufügen mit config: {connection_config}"
            )

            # Schritt 1: Datenbanktyp erkennen (schnell, außerhalb Lock)
            db_type = self._detect_database_type(connection_config)
            logger.info(f"Erkannter Datenbanktyp für '{name}': {db_type}")

            # Schritt 2: Konfiguration thread-sicher speichern (OHNE Connector-Erstellung)
            with self._lock:
                # Doppelte Prüfung innerhalb des Locks
                if name in self.connection_configs:
                    db_logger.log_warning(
                        f"Connection '{name}' already exists",
                        "duplicate_connection_attempt",
                    )
                    logger.warning(
                        f"Verbindung '{name}' existiert bereits, überspringe Hinzufügen"
                    )
                    return False

                # Nur Konfiguration speichern, Connector wird bei Bedarf erstellt (lazy loading)
                self.connection_configs[name] = {
                    **connection_config,
                    "type": db_type.value,
                }
                logger.info(f"Konfiguration für '{name}' gespeichert")

            # Schritt 3: Datei außerhalb des Locks speichern
            self._save_connections()

            # Enhanced Logging: Success
            db_logger.log_database_operation(
                "add_connection_completed",
                name,
                success=True,
                details={"type": db_type.value},
            )

            logger.info(
                f"Datenbankverbindung '{name}' ({db_type.value}) erfolgreich hinzugefügt und gespeichert"
            )
            return True

        except Exception as e:
            # Enhanced Logging: Error
            db_logger.log_error(e, f"Failed to add connection {name}")
            logger.error(
                f"Fehler beim Hinzufügen der Verbindung '{name}': {e}", exc_info=True
            )
            return False

    def add_connection_simple(
        self, name: str, db_type: str, connection_string: str
    ) -> bool:
        """Ultra-einfaches Speichern einer Verbindung - SOFORT und ohne Umwege"""
        try:
            # MySQL/MariaDB Connection String korrigieren falls notwendig
            if db_type.lower() in ["mysql", "mariadb"]:
                connection_string = self._fix_mysql_connection_string(connection_string)

            # Direkt in die Konfiguration einfügen
            with self._lock:
                if name in self.connection_configs:
                    return False  # Bereits vorhanden

                self.connection_configs[name] = {
                    "connection_string": connection_string,
                    "type": db_type,
                }

            # Sofort in JSON speichern
            self._save_connections()
            logger.info(f"[OK] Verbindung '{name}' ultra-schnell gespeichert")
            return True

        except Exception as e:
            logger.error(f"❌ Fehler beim einfachen Speichern: {e}")
            return False

    def _fix_mysql_connection_string(self, connection_string: str) -> str:
        """
        Korrigiert MySQL Connection Strings automatisch, um MySQLdb-Fehler zu vermeiden.
        Wandelt 'mysql://' in 'mysql+mysqlconnector://' um.
        """
        if connection_string.startswith("mysql://"):
            # Prüfe, ob bereits ein Treiber spezifiziert ist
            if (
                "+mysqlconnector://" not in connection_string
                and "+pymysql://" not in connection_string
            ):
                # Ersetze mysql:// durch mysql+mysqlconnector://
                corrected = connection_string.replace(
                    "mysql://", "mysql+mysqlconnector://", 1
                )
                logger.info(
                    f"MySQL Connection String automatisch korrigiert: {connection_string} -> {corrected}"
                )
                return corrected

        return connection_string

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
        """Erstellt passenden Connector für Datenbanktyp - mit produktionsreifer Fehlerbehandlung"""
        try:
            # MySQL Connection String korrigieren falls notwendig
            connection_string = config["connection_string"]
            if db_type == DatabaseType.MYSQL:
                connection_string = self._fix_mysql_connection_string(connection_string)
                # Korrigierten String in der Konfiguration verwenden
                config = {**config, "connection_string": connection_string}

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
                    connection_string=config["connection_string"],
                    db_type=connector_db_type,
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

        except ImportError as e:
            # Fehlende Abhängigkeiten
            error_msg = f"Fehlende Abhängigkeit für {db_type.value}: {str(e)}"
            logger.error(error_msg)
            raise ImportError(error_msg)
        except Exception as e:
            # Connector-Erstellung fehlgeschlagen
            error_msg = (
                f"Connector-Erstellung für {db_type.value} fehlgeschlagen: {str(e)}"
            )
            logger.error(error_msg)
            raise Exception(error_msg)

    def get_connection(self, name: str):
        """Gibt Datenbankverbindung zurück - mit lazy loading"""
        with self._lock:
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
                    logger.error(
                        f"Fehler beim Lazy-Loading der Verbindung '{name}': {e}"
                    )
                    raise

        return self.connections[name]

    def remove_connection(self, name: str) -> bool:
        """Entfernt eine Datenbankverbindung - sicher und ohne Race Conditions"""
        try:
            with self._lock:
                # Schritt 1: Verbindung aus dem Speicher schließen und entfernen
                if name in self.connections:
                    # Verbindung schließen falls möglich
                    if hasattr(self.connections[name], "close"):
                        self.connections[name].close()
                    del self.connections[name]
                    logger.info(f"Aktive Verbindung '{name}' geschlossen")

                # Schritt 2: Konfiguration aus dem Speicher entfernen
                if name in self.connection_configs:
                    del self.connection_configs[name]
                    logger.info(f"Konfiguration für '{name}' aus dem Speicher entfernt")
                else:
                    logger.warning(
                        f"Verbindung '{name}' nicht in Konfiguration gefunden"
                    )
                    return False

            # Schritt 3: Datei außerhalb des Locks aktualisieren
            self._save_connections()
            logger.info(
                f"Verbindung '{name}' vollständig entfernt und Datei gespeichert"
            )
            return True

        except Exception as e:
            logger.error(f"Fehler beim Entfernen der Verbindung '{name}': {e}")
            return False

    def list_connections(self) -> List[Dict[str, Any]]:
        """Listet alle verfügbaren Verbindungen auf - sichere Kopie aller Informationen"""
        with self._lock:
            # Erstelle eine vollständige und sichere Kopie aller Verbindungsinformationen
            connections_copy = []
            for name, config in self.connection_configs.items():
                connection_info = {
                    "name": name,
                    "type": config.get("type", "unknown"),
                    "connection_string": config.get("connection_string", ""),
                    "status": "connected"
                    if name in self.connections
                    else "disconnected",
                }
                connections_copy.append(connection_info)

            return connections_copy

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
        self, name: str, db_type: str, conn_string: str
    ) -> Dict[str, Any]:
        """
        Testet eine Datenbankverbindung - Produktionsreife Version.
        Führt nur minimale, schnelle Operationen aus.
        """
        start_time = time.time()

        # Enhanced Logging: Start
        db_logger.log_database_operation(
            "test_connection_started",
            name,
            success=True,
            details={"db_type": db_type, "conn_string_length": len(conn_string)},
        )

        try:
            # MySQL/MariaDB Connection String korrigieren falls notwendig
            if db_type.lower() in ["mysql", "mariadb"]:
                conn_string = self._fix_mysql_connection_string(conn_string)

            # Temporären Connector erstellen, ohne ihn zu speichern
            config = {"connection_string": conn_string, "type": db_type}
            detected_type = self._detect_database_type(config)
            connector = self._create_connector(detected_type, config)

            # Nur minimale Verbindungstests - keine zeitaufwändigen Schema-Abfragen
            if detected_type == DatabaseType.MONGODB:
                # MongoDB: Nur Ping - kein list_databases()
                connector.client.admin.command("ping")
                message = "✅ MongoDB Verbindung erfolgreich"
            else:  # SQL-Datenbanken
                # SQL: Nur SELECT 1 - kein get_table_names()
                connector.execute_query("SELECT 1")
                message = f"✅ {db_type.upper()}-Datenbank Verbindung erfolgreich"

            elapsed = round(time.time() - start_time, 2)

            # Enhanced Logging: Success
            db_logger.log_connection_event("test", name, success=True)
            db_logger.log_database_operation(
                "test_connection_completed",
                name,
                success=True,
                details={"duration": elapsed, "db_type": db_type},
            )

            return {
                "status": "success",
                "message": f"{message} (Dauer: {elapsed}s)",
            }

        except Exception as e:
            elapsed = round(time.time() - start_time, 2)

            # Enhanced Logging: Error
            db_logger.log_connection_event("test", name, success=False)
            db_logger.log_error(e, f"Connection test failed for {name}")

            logger.error(
                f"Verbindungstest für '{name}' fehlgeschlagen: {e}", exc_info=False
            )
            return {
                "status": "error",
                "message": f"❌ Verbindungsfehler nach {elapsed}s: {str(e)}",
                "details": {
                    "connection_name": name,
                    "error": str(e),
                },
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

    def get_schema_info(
        self, connection_name: str, database: str = None
    ) -> Dict[str, Any]:
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
                    "collections": {},
                }

                for collection in collections:
                    # Sample-Dokument für Schema-Erkennung
                    sample = connector.execute_raw_query(
                        database, collection, "find_one", {}, {}
                    )
                    if sample:
                        schema_info["collections"][collection] = {
                            "sample_fields": list(sample.keys()),
                            "sample_document": sample,
                        }
                    else:
                        schema_info["collections"][collection] = {
                            "sample_fields": [],
                            "sample_document": None,
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
                schema_info = {"type": db_type, "tables": {}}

                if db_type == "sqlite":
                    # Für SQLite: Hole Spalten für jede Tabelle einzeln
                    for _, row in result_df.iterrows():
                        table_name = row["table_name"]
                        pragma_query = f"PRAGMA table_info({table_name})"
                        columns_df = connector.execute_query(pragma_query)

                        schema_info["tables"][table_name] = {"columns": []}

                        for _, col_row in columns_df.iterrows():
                            schema_info["tables"][table_name]["columns"].append(
                                {
                                    "name": col_row["name"],
                                    "type": col_row["type"],
                                    "nullable": not bool(col_row["notnull"]),
                                    "default": col_row["dflt_value"],
                                }
                            )
                else:
                    # Für andere SQL-DBs: Gruppiere nach Tabelle
                    for _, row in result_df.iterrows():
                        table_name = row["table_name"]
                        if table_name not in schema_info["tables"]:
                            schema_info["tables"][table_name] = {"columns": []}

                        schema_info["tables"][table_name]["columns"].append(
                            {
                                "name": row["column_name"],
                                "type": row["data_type"],
                                "nullable": row.get("is_nullable", "YES") == "YES",
                                "default": row.get("column_default"),
                            }
                        )

                return schema_info

        except Exception as e:
            logger.error(
                f"Schema-Introspection für {connection_name} fehlgeschlagen: {e}"
            )
            return {
                "type": db_type,
                "error": str(e),
                "tables": {} if db_type != "mongodb" else {},
                "collections": {} if db_type == "mongodb" else {},
            }

    def connection_exists(self, name: str) -> bool:
        """Prüft thread-sicher, ob eine Verbindung bereits existiert"""
        with self._lock:
            return name in self.connection_configs

    def get_diagnostic_info(self) -> Dict[str, Any]:
        """Gibt Diagnoseinformationen zurück für Debugging"""
        with self._lock:
            return {
                "config_count": len(self.connection_configs),
                "active_connections": len(self.connections),
                "config_names": list(self.connection_configs.keys()),
                "active_names": list(self.connections.keys()),
                "config_file": self.config_file,
                "file_exists": os.path.exists(self.config_file),
                "lock_acquired": True,  # Wenn wir hier sind, haben wir den Lock
            }
