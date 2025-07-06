import pandas as pd
import logging
import json
import os
from typing import Dict, List, Any
import pymongo
import sqlalchemy as sa

logger = logging.getLogger(__name__)


class SimpleDBManager:
    def __init__(self, config_file: str = "db_connections.json"):
        self.connection_configs = {}
        self.config_file = config_file
        self._load_connections()

    def _load_connections(self):
        if os.path.exists(self.config_file):
            with open(self.config_file, "r", encoding="utf-8") as f:
                self.connection_configs = json.load(f)

    def extract_data(
        self, connection_name: str, query_config: Dict[str, Any]
    ) -> pd.DataFrame:
        config = self.connection_configs[connection_name]
        if config["type"] == "mysql":
            engine = sa.create_engine(config["connection_string"])
            return pd.read_sql(query_config["query"], engine)
        elif config["type"] == "mongodb":
            client = pymongo.MongoClient(config["connection_string"])
            db_name = config["connection_string"].split("/")[-1] or "my_test_db"
            db = client[db_name]
            collection = db[query_config["collection"]]
            data = list(
                collection.find(query_config.get("query", {})).limit(
                    query_config.get("limit", 1000)
                )
            )
            client.close()
            return pd.DataFrame(data)
        else:
            raise ValueError(f"Nicht unterstützter DB-Typ: {config['type']}")

    def load_data(
        self, connection_name: str, df: pd.DataFrame, target_config: Dict[str, Any]
    ):
        config = self.connection_configs[connection_name]
        if config["type"] == "mongodb":
            client = pymongo.MongoClient(config["connection_string"])
            db_name = config["connection_string"].split("/")[-1] or "my_test_db"
            collection_name = target_config.get("collection", "NEUTEST")
            client[db_name][collection_name].insert_many(df.to_dict("records"))
        else:
            engine = sa.create_engine(config["connection_string"])
            df.to_sql(
                target_config["table"],
                engine,
                if_exists=target_config.get("if_exists", "replace"),
                index=False,
            )


def etl_pipeline():
    try:
        logger.info("Starte ETL-Pipeline...")
        db_manager = SimpleDBManager()

        # EXTRACT
        logger.info("Extrahiere Daten aus Quelle...")
        extract_config = {"query": "SELECT * FROM users_test"}
        df = db_manager.extract_data("MySQLTEST", extract_config)
        logger.info(f" extrahiert: {len(df)} Datensätze")

        # TRANSFORM
        logger.info("Transformiere Daten...")
        df["age"] = df["age"] + 2
        logger.info(" podraten! Bitte beachte die Bedeutung von age!")

        # LOAD
        logger.info("Lade Daten in Ziel...")
        load_config = {"collection": "NEUTEST", "operation": "replace"}
        db_manager.load_data("mong4", df, load_config)
        logger.info("Daten erfolgreich geladen")

        logger.info("ETL-Pipeline erfolgreich abgeschlossen")
        return [df, "NEUTEST"]

    except Exception as e:
        logger.error(f"ETL-Pipeline Fehler: {e}")
        raise


if __name__ == "__main__":
    result = etl_pipeline()
    print(f"ETL abgeschlossen. Verarbeitete Datensätze: {len(result[0])}")
