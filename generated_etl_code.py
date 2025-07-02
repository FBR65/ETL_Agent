import pandas as pd
import logging
from etl_agent.database_manager import DatabaseManager

logger = logging.getLogger(__name__)


def etl_pipeline():
    try:
        db_manager = DatabaseManager()

        # EXTRACT
        logger.info("Extrahiere Daten aus der Tabelle users_test")
        query_config = {"query": "SELECT id, name, age FROM users_test"}
        df_users = db_manager.extract_data("Jup", query_config)

        # TRANSFORM
        logger.info("Füge dem Alter 2 hinzu")
        df_users["age"] += 2

        # LOAD
        target_connection = "Jup"
        load_config = {"table": "NEUTEST", "if_exists": "replace"}

        logger.info(f"Lade Daten in die Tabelle NEUTEST auf {target_connection}")
        db_manager.load_data(target_connection, df_users, load_config)

        logger.info("ETL abgeschlossen")
    except Exception as e:
        logger.error(f" ETLPipeline Fehler: {e}")


if __name__ == "__main__":
    etl_pipeline()
