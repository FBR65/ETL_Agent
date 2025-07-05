"""
Enhanced logging utilities for ETL Agent
Provides structured logging with context for better monitoring and debugging
"""

import logging
import hashlib
import time
from typing import Dict, Any, Optional
from functools import wraps


class ETLDesignerLogger:
    """Enhanced logger for ETL Designer with structured logging capabilities"""

    def __init__(self, name: str = "etl_designer"):
        self.logger = logging.getLogger(name)
        self.metrics = {
            "queries_executed": 0,
            "successful_queries": 0,
            "failed_queries": 0,
            "total_response_time": 0.0,
            "ai_tokens_used": 0,
            "active_connections": 0,
        }

    def log_user_action(self, action: str, details: Dict[str, Any]):
        """Log user actions with structured context"""
        message = f"🎯 User Action: {action} | Details: {self._format_details(details)}"
        self.logger.info(message)
        print_enhanced_log(message, "INFO")

    def log_ai_interaction(
        self, prompt: str, response: str, tokens_used: int, duration: float
    ):
        """Log AI interactions with performance metrics"""
        self.metrics["ai_tokens_used"] += tokens_used

        prompt_summary = f"{prompt[:100]}..." if len(prompt) > 100 else prompt
        response_summary = f"{response[:100]}..." if len(response) > 100 else response

        message = (
            f"🤖 AI Interaction | "
            f"Prompt: {prompt_summary} | "
            f"Response: {response_summary} | "
            f"Tokens: {tokens_used} | "
            f"Duration: {duration:.2f}s"
        )
        self.logger.info(message)
        # ALSO print to terminal with enhanced visibility
        print_enhanced_log(message)

    def log_query_execution(
        self, query: str, duration: float, rows_returned: int, success: bool = True
    ):
        """Log query execution with performance metrics"""
        self.metrics["queries_executed"] += 1
        self.metrics["total_response_time"] += duration

        if success:
            self.metrics["successful_queries"] += 1
            query_hash = hashlib.md5(query.encode()).hexdigest()[:8]
            self.logger.info(
                f"✅ Query Executed | "
                f"Hash: {query_hash} | "
                f"Rows: {rows_returned} | "
                f"Duration: {duration:.2f}s"
            )
        else:
            self.metrics["failed_queries"] += 1
            self.logger.error(
                f"❌ Query Failed | Query: {query[:100]}... | Duration: {duration:.2f}s"
            )

    def log_database_operation(
        self,
        operation: str,
        database: str,
        success: bool = True,
        details: Optional[Dict] = None,
    ):
        """Log database operations"""
        status = "✅" if success else "❌"
        details_str = f" | Details: {self._format_details(details)}" if details else ""

        self.logger.info(
            f"{status} DB Operation: {operation} | Database: {database}{details_str}"
        )

    def log_connection_event(
        self, event: str, connection_name: str, success: bool = True
    ):
        """Log connection events (connect, disconnect, test)"""
        if event == "connect" and success:
            self.metrics["active_connections"] += 1
        elif event == "disconnect" and success:
            self.metrics["active_connections"] = max(
                0, self.metrics["active_connections"] - 1
            )

        status = "✅" if success else "❌"
        self.logger.info(
            f"{status} Connection {event.title()}: {connection_name} | "
            f"Active: {self.metrics['active_connections']}"
        )

    def log_error(self, error: Exception, context: str = ""):
        """Log errors with context"""
        context_str = f" | Context: {context}" if context else ""
        self.logger.error(
            f"❌ Error: {type(error).__name__} | Message: {str(error)}{context_str}"
        )

    def log_warning(self, message: str, context: str = ""):
        """Log warnings with context"""
        context_str = f" | Context: {context}" if context else ""
        self.logger.warning(f"⚠️ Warning: {message}{context_str}")

    def log_session_metrics(self):
        """Log session performance metrics"""
        avg_response_time = self.metrics["total_response_time"] / max(
            1, self.metrics["queries_executed"]
        )
        success_rate = (
            self.metrics["successful_queries"]
            / max(1, self.metrics["queries_executed"])
            * 100
        )

        self.logger.info(
            f"📈 Session Metrics | "
            f"Queries: {self.metrics['queries_executed']} | "
            f"Success Rate: {success_rate:.1f}% | "
            f"Avg Response: {avg_response_time:.2f}s | "
            f"AI Tokens: {self.metrics['ai_tokens_used']} | "
            f"Connections: {self.metrics['active_connections']}"
        )

    def _format_details(self, details: Dict[str, Any]) -> str:
        """Format details dictionary for logging"""
        if not details:
            return ""

        formatted = []
        for key, value in details.items():
            if isinstance(value, str) and len(value) > 50:
                value = f"{value[:50]}..."
            formatted.append(f"{key}={value}")

        return ", ".join(formatted)


def log_execution_time(logger: ETLDesignerLogger, operation: str):
    """Decorator to log execution time of functions"""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                logger.logger.info(f"⏱️ {operation} completed in {duration:.2f}s")
                return result
            except Exception as e:
                duration = time.time() - start_time
                logger.log_error(e, f"{operation} failed after {duration:.2f}s")
                raise

        return wrapper

    return decorator


def setup_etl_logging(log_level: str = "INFO", log_file: Optional[str] = None):
    """Setup enhanced logging configuration for ETL Agent"""

    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_file or "etl_agent.log", encoding="utf-8"),
        ],
    )

    # Set specific loggers
    logging.getLogger("etl_designer").setLevel(logging.INFO)
    logging.getLogger("database_manager").setLevel(logging.INFO)
    logging.getLogger("etl_core").setLevel(logging.INFO)

    return ETLDesignerLogger()


def print_enhanced_log(message: str, level: str = "INFO"):
    """Print enhanced log message to terminal with visual emphasis"""
    colors = {
        "INFO": "\033[94m",  # Blue
        "WARNING": "\033[93m",  # Yellow
        "ERROR": "\033[91m",  # Red
        "SUCCESS": "\033[92m",  # Green
    }
    reset = "\033[0m"

    color = colors.get(level.upper(), colors["INFO"])
    print(f"{color}🔔 ETL ENHANCED: {message}{reset}")
