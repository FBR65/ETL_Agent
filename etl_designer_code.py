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
            "Jup": {"type": "mysql", "connection_string": "mysql+mysqlconnector://dataanalyzer:dataanalyzer_pwd@localhost:3306/my_test_db"},
            "mong4": {"type": "mongodb", "connection_string": "mongodb://user:pass@localhost:27017/my_test_db"}
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
            logger.info(f"✅ MongoDB: {len(records)} Datensätze in '{target_config['collection']}' geladen")
        else:
            engine = sa.create_engine(config["connection_string"])
            df.to_sql(target_config["table"], engine, if_exists=target_config.get("if_exists", "replace"), index=False)
            engine.dispose()
            logger.info(f"✅ SQL: {len(df)} Datensätze in '{target_config['table']}' geladen")

def etl_pipeline():
    try:
        logger.info("=== ETL-Pipeline gestartet ===")
        db_manager = SimpleDBManager()
        
        # EXTRACT - ECHTE TABELLE VERWENDEN
        logger.info("Phase 1: Daten extrahieren")
        extract_config = {"query": "SELECT * FROM users_test"}
        df = db_manager.extract_data("Jup", extract_config)
        logger.info(f"Extrahiert: {len(df)} Datensätze")
        
        # TRANSFORM - ECHTE SPALTE VERWENDEN
        logger.info("Phase 2: Daten transformieren")
        df['age'] = df['age'] + 2
        logger.info("Transformation abgeschlossen")
        
        # LOAD - ECHTE COLLECTION VERWENDEN
        logger.info("Phase 3: Daten laden")
        load_config = {"collection": "NEUTEST", "operation": "replace"}
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