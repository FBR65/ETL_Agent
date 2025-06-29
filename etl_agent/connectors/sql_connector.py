"""
SQL Database Connector für ETL-Agent
Unterstützt PostgreSQL, MySQL, MariaDB, SQLite, SQL Server
"""

import logging
from typing import Dict, List, Any, Optional
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)


class SQLConnector:
    """SQL-Datenbank Connector für ETL-Operationen"""

    def __init__(self, connection_string: str, db_type: str, **options):
        """Initialisiert den SQL-Connector"""
        self.connection_string = self._fix_connection_string(connection_string, db_type)
        self.db_type = db_type
        self.options = options
        self.engine = None
        self._connect()

    def _fix_connection_string(self, conn_str: str, db_type: str) -> str:
        """Korrigiert Connection String für richtige Driver"""
        if db_type in ["mysql", "mariadb"]:
            # MySQL/MariaDB: Verwende mysql-connector-python statt MySQLdb
            if conn_str.startswith("mysql://") or conn_str.startswith("mariadb://"):
                # Ersetze mysql:// oder mariadb:// mit mysql+mysqlconnector://
                fixed_str = conn_str.replace("mysql://", "mysql+mysqlconnector://")
                fixed_str = fixed_str.replace("mariadb://", "mysql+mysqlconnector://")
                logger.info(f"Connection String korrigiert: {conn_str} -> {fixed_str}")
                return fixed_str

        return conn_str

    def _connect(self):
        """Stellt Verbindung zur SQL-Datenbank her"""
        try:
            logger.info(
                f"Verbinde zu {self.db_type}-Datenbank: {self.connection_string}"
            )

            # Engine-Optionen basierend auf Datenbanktyp
            engine_options = {"echo": False, **self.options}

            if self.db_type == "sqlite":
                engine_options["check_same_thread"] = False

            self.engine = create_engine(self.connection_string, **engine_options)

            # Verbindung testen
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))

            logger.info(f"{self.db_type}-Verbindung erfolgreich etabliert")

        except Exception as e:
            logger.error(f"Fehler bei {self.db_type}-Datenbankverbindung: {e}")
            raise

    def get_table_names(self) -> List[str]:
        """Gibt alle Tabellennamen zurück"""
        try:
            inspector = inspect(self.engine)
            tables = inspector.get_table_names()
            logger.debug(f"Gefundene Tabellen: {tables}")
            return tables
        except Exception as e:
            logger.error(f"Fehler beim Abrufen der Tabellennamen: {e}")
            return []

    def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """Gibt das Schema einer Tabelle zurück"""
        try:
            inspector = inspect(self.engine)
            columns = inspector.get_columns(table_name)

            schema = []
            for col in columns:
                schema.append(
                    {
                        "name": col["name"],
                        "type": str(col["type"]),
                        "nullable": col.get("nullable", True),
                        "default": col.get("default"),
                        "primary_key": col.get("primary_key", False),
                    }
                )

            return schema
        except Exception as e:
            logger.error(
                f"Fehler beim Abrufen des Schemas für Tabelle {table_name}: {e}"
            )
            return []

    def execute_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Führt SQL-Abfrage aus und gibt DataFrame zurück"""
        try:
            logger.info(f"Führe Abfrage aus: {query[:100]}...")

            with self.engine.connect() as conn:
                if params:
                    result = conn.execute(text(query), params)
                else:
                    result = conn.execute(text(query))

                # DataFrame aus Result erstellen
                df = pd.DataFrame(result.fetchall(), columns=result.keys())

                logger.info(f"Abfrage erfolgreich: {len(df)} Zeilen zurückgegeben")
                return df

        except Exception as e:
            logger.error(f"Fehler bei SQL-Abfrage: {e}")
            raise

    def insert_dataframe(
        self, df: pd.DataFrame, table_name: str, if_exists: str = "append"
    ):
        """Fügt DataFrame in Tabelle ein"""
        try:
            logger.info(
                f"Lade {len(df)} Zeilen in Tabelle {table_name} (if_exists={if_exists})"
            )

            df.to_sql(
                name=table_name,
                con=self.engine,
                if_exists=if_exists,
                index=False,
                method="multi",  # Bulk insert für bessere Performance
            )

            logger.info(f"DataFrame erfolgreich in {table_name} geladen")

        except Exception as e:
            logger.error(f"Fehler beim Laden in Tabelle {table_name}: {e}")
            raise

    def close(self):
        """Schließt SQL-Verbindung"""
        if self.engine:
            self.engine.dispose()
            logger.info(f"{self.db_type}-Verbindung geschlossen")
