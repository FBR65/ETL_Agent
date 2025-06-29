"""
ETL Processor - Kernlogik für ETL-Operationen
"""

import logging
from typing import Dict, List, Any, Optional, Union
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)


class ETLProcessor:
    """ETL-Verarbeitungslogik"""

    def __init__(self):
        self.operations_log = []

    def transform_data(
        self, df: pd.DataFrame, transformations: List[Dict[str, Any]]
    ) -> pd.DataFrame:
        """Wendet Transformationen auf DataFrame an"""
        result_df = df.copy()

        for transformation in transformations:
            operation = transformation.get("operation")

            try:
                if operation == "filter":
                    result_df = self._apply_filter(result_df, transformation)
                elif operation == "rename":
                    result_df = self._apply_rename(result_df, transformation)
                elif operation == "aggregate":
                    result_df = self._apply_aggregation(result_df, transformation)
                elif operation == "join":
                    result_df = self._apply_join(result_df, transformation)
                elif operation == "convert_type":
                    result_df = self._convert_types(result_df, transformation)

                self.operations_log.append(
                    {
                        "timestamp": datetime.now(),
                        "operation": operation,
                        "status": "success",
                        "rows_before": len(df),
                        "rows_after": len(result_df),
                    }
                )

            except Exception as e:
                logger.error(f"Transformation {operation} fehlgeschlagen: {e}")
                self.operations_log.append(
                    {
                        "timestamp": datetime.now(),
                        "operation": operation,
                        "status": "error",
                        "error": str(e),
                    }
                )
                raise

        return result_df

    def _apply_filter(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """Wendet Filter auf DataFrame an"""
        condition = config.get("condition")
        if condition:
            return df.query(condition)
        return df

    def _apply_rename(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """Benennt Spalten um"""
        column_mapping = config.get("mapping", {})
        return df.rename(columns=column_mapping)

    def _apply_aggregation(
        self, df: pd.DataFrame, config: Dict[str, Any]
    ) -> pd.DataFrame:
        """Führt Aggregation durch"""
        group_by = config.get("group_by", [])
        agg_functions = config.get("aggregations", {})

        if group_by and agg_functions:
            return df.groupby(group_by).agg(agg_functions).reset_index()
        return df

    def _apply_join(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """Führt Join-Operation durch"""
        # Placeholder für Join-Logik
        # Würde normalerweise zweiten DataFrame und Join-Parameter benötigen
        logger.warning("Join-Operation noch nicht implementiert")
        return df

    def _convert_types(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """Konvertiert Datentypen"""
        type_mapping = config.get("types", {})
        for column, dtype in type_mapping.items():
            if column in df.columns:
                df[column] = df[column].astype(dtype)
        return df

    def get_operations_summary(self) -> Dict[str, Any]:
        """Gibt Zusammenfassung der Operationen zurück"""
        return {
            "total_operations": len(self.operations_log),
            "successful_operations": len(
                [op for op in self.operations_log if op["status"] == "success"]
            ),
            "failed_operations": len(
                [op for op in self.operations_log if op["status"] == "error"]
            ),
            "operations": self.operations_log,
        }
