"""
SQL Database Connector für ETL-Agent
"""

import logging
from typing import Dict, List, Any, Optional
import sqlalchemy as sa
from sqlalchemy import create_engine, inspect, text
import pandas as pd

logger = logging.getLogger(__name__)


class DatabaseConnector:
    """SQL-Datenbank Connector für ETL-Operationen"""

    def __init__(self, connection_string: str):
        """Initialisiert den Datenbank-Connector"""
        self.connection_string = connection_string
        self.engine = None
        self.inspector = None
        self._connect()

    def _connect(self):
        """Stellt Verbindung zur Datenbank her"""
        try:
            self.engine = create_engine(self.connection_string)
            self.inspector = inspect(self.engine)
            logger.info("SQL-Datenbankverbindung hergestellt")
        except Exception as e:
            logger.error(f"Fehler bei Datenbankverbindung: {e}")
            raise

    def get_table_names(self) -> List[str]:
        """Gibt alle Tabellennamen zurück"""
        try:
            return self.inspector.get_table_names()
        except Exception as e:
            logger.error(f"Fehler beim Abrufen der Tabellennamen: {e}")
            return []

    def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """Gibt das Schema einer Tabelle zurück"""
        try:
            columns = self.inspector.get_columns(table_name)
            return [
                {
                    "name": col["name"],
                    "type": str(col["type"]),
                    "nullable": col.get("nullable", True),
                    "default": col.get("default"),
                }
                for col in columns
            ]
        except Exception as e:
            logger.error(
                f"Fehler beim Abrufen des Tabellenschemas für {table_name}: {e}"
            )
            return []

    def execute_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Führt eine SQL-Abfrage aus und gibt DataFrame zurück"""
        try:
            with self.engine.connect() as conn:
                result = pd.read_sql(text(query), conn, params=params)
            return result
        except Exception as e:
            logger.error(f"Fehler bei Abfrageausführung: {e}")
            raise

    def insert_dataframe(
        self, df: pd.DataFrame, table_name: str, if_exists: str = "append"
    ):
        """Fügt DataFrame in Tabelle ein"""
        try:
            df.to_sql(table_name, self.engine, if_exists=if_exists, index=False)
            logger.info(f"DataFrame erfolgreich in {table_name} eingefügt")
        except Exception as e:
            logger.error(f"Fehler beim Einfügen in {table_name}: {e}")
            raise

    def get_sample_data(self, table_name: str, limit: int = 5) -> pd.DataFrame:
        """Gibt Beispieldaten einer Tabelle zurück"""
        query = f"SELECT * FROM {table_name} LIMIT {limit}"
        return self.execute_query(query)

    def close(self):
        """Schließt die Datenbankverbindung"""
        if self.engine:
            self.engine.dispose()
            logger.info("Datenbankverbindung geschlossen")
