import pandas as pd
import pymongo
from typing import Dict, Any
class DatabaseManager:
    def connect_to_ db(self, connection_name: str) -> sa.engine:
        """Verbinde mit der SQL- oder NoSQL-Datenbank und returne das Engine-Objekt"""
        raise NotImplementedError("Implement this method in subclass!")
    def extract_ data(self, connection_name: str, query: str) -> pd.DataFrame:
        """Extractor Daten aus der SQL- oder NoSQL-Datenbank und returne sie als Pandas Dataframe."""
        raise NotImplementedError("Implement this method in subclass!")
    def load_ data(self, connection_name: str, df: pd.DataFrame, target_config: Dict[str, Any]) -> int:
        """Lädt die Daten aus einem Pandas Dataframe in eine SQL- oder NoSQL-Tabelle und returne die Anzahl der affectierten Zeilen."""
        raise NotImplementedError("Implement this method in subclass!")
class SQLManager(DatabaseManager):
    def connect_to_db(self, connection_name: str) -> sa.engine:
        """Verbinde mit einer SQL-Datenbank und returne das Engine-Objekt"""
        # Verwenden des dataanalyzer API
        return sa.engine.create_engine(config["connection-string"])
    def extract_data(self, connection_name: str, query: str) -> pd.DataFrame:
        """Extractor Daten aus der SQL-Datenbank und returne sie als Pandas Dataframe."""
        # Verwenden des dataanalyzer API
        engine = self.connect_to_db(connection_name)
        return pd.read_sql(query, engine)
    def load_data(self, connection_name: str, df: pd.DataFrame, target_config: Dict[str, Any]) -> int:
        """Lädt die Daten aus einem Pandas Dataframe in eine SQL-Tabelle und returne die Anzahl der affectierten Zeilen."""
        engine = self.connect_to_db(connection_name)
        df.to_sql(target_config["table"], engine, if_exists=target_config.get("if_exists", "replace"), index=False)
        return len(df)
class NoSQLManager(DatabaseManager):
    def connect_to_ db(self, connection_name: str) -> pymongo.collection:
        """Verbinde mit einer NoSQL-Datenbank und returne das Collection-Objekt"""
        # Verwenden des dataanalyzer API
        return pymongo[conn["database"]]
    def extract_data(self, connection_name: str, query: Dict[str, Any]) -> pd.DataFrame:
        """Extractor Daten aus der NoSQL-Datenbank und returne sie als Pandas Dataframe."""
        # Verwenden des dataanalyzer API
        collection = self.connect_to_db(connection_name)[conn["collection"]]
        return pd.DataFrame(list(collection.find()))
    def load_data(self, connection_name: str, df: pd.DataFrame, target_config: Dict[str, Any]) -> int:
        """Lädt die Daten aus einem Pandas Dataframe in eine NoSQL-Collection und returne die Anzahl der affectierten Zeilen."""
        # Verwenden des dataanalyzer API
        collection = self.connect_to_db(connection_name)[target_config["collection"]]
        for row in df.to_dict("records"):
            collection.insert_one(row)
        return len(df)
