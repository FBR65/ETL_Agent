"""
ETL Agent Package
"""

# Stelle sicher, dass alle Module korrekt importiert werden können
from .database_manager import DatabaseManager
from .etl_agent_core import ETLAgent, ETLRequest, ETLResponse

__all__ = ["DatabaseManager", "ETLAgent", "ETLRequest", "ETLResponse"]

__version__ = "0.1.0"
__author__ = "ETL Agent Team"
__description__ = "ETL Agent mit PydanticAI, A2A und MCP Integration"
