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
        message = f"USER ACTION: {action} | Details: {self._format_details(details)}"
        self.logger.info(message)
        print_enhanced_log(message, "INFO")

    def log_ai_interaction(
        self, prompt: str, response: str, tokens_used: int, duration: float
    ):
        """Log AI interactions with performance metrics and quality validation"""
        self.metrics["ai_tokens_used"] += tokens_used

        prompt_summary = f"{prompt[:100]}..." if len(prompt) > 100 else prompt
        response_summary = f"{response[:100]}..." if len(response) > 100 else response

        # CRITICAL: Check for bad AI responses - NO FALLBACKS!
        if self._is_bad_ai_response(response):
            critical_message = (
                f"CRITICAL AI FAILURE | "
                f"Prompt: {prompt_summary} | "
                f"BAD Response: {response_summary} | "
                f"Tokens: {tokens_used} | "
                f"Duration: {duration:.2f}s | "
                f"QUALITY: UNACCEPTABLE"
            )
            self.logger.critical(critical_message)
            print_enhanced_log(critical_message, "CRITICAL")

            # Log the full bad response for debugging
            self.logger.error(f"FULL BAD AI RESPONSE: {response}")
            return

        message = (
            f"AI INTERACTION | "
            f"Prompt: {prompt_summary} | "
            f"Response: {response_summary} | "
            f"Tokens: {tokens_used} | "
            f"Duration: {duration:.2f}s"
        )
        self.logger.info(message)
        print_enhanced_log(message, "INFO")

    def log_query_execution(
        self, query: str, duration: float, rows_returned: int, success: bool = True
    ):
        """Log query execution with performance metrics"""
        self.metrics["queries_executed"] += 1
        self.metrics["total_response_time"] += duration

        if success:
            self.metrics["successful_queries"] += 1
            query_hash = hashlib.md5(query.encode()).hexdigest()[:8]
            message = (
                f"QUERY EXECUTED | "
                f"Hash: {query_hash} | "
                f"Rows: {rows_returned} | "
                f"Duration: {duration:.2f}s"
            )
            self.logger.info(message)
            print_enhanced_log(message, "SUCCESS")
        else:
            self.metrics["failed_queries"] += 1
            message = (
                f"QUERY FAILED | Query: {query[:100]}... | Duration: {duration:.2f}s"
            )
            self.logger.error(message)
            print_enhanced_log(message, "ERROR")

    def log_database_operation(
        self,
        operation: str,
        database: str,
        success: bool = True,
        details: Optional[Dict] = None,
    ):
        """Log database operations"""
        status = "SUCCESS" if success else "ERROR"
        details_str = f" | Details: {self._format_details(details)}" if details else ""

        message = (
            f"DB OPERATION {status}: {operation} | Database: {database}{details_str}"
        )
        self.logger.info(message)
        print_enhanced_log(message, "SUCCESS" if success else "ERROR")

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

        status = "SUCCESS" if success else "ERROR"
        message = (
            f"CONNECTION {event.upper()} {status}: {connection_name} | "
            f"Active: {self.metrics['active_connections']}"
        )
        self.logger.info(message)
        print_enhanced_log(message, "SUCCESS" if success else "ERROR")

    def log_error(self, error: Exception, context: str = ""):
        """Log errors with context"""
        context_str = f" | Context: {context}" if context else ""
        message = f"ERROR: {type(error).__name__} | Message: {str(error)}{context_str}"
        self.logger.error(message)
        print_enhanced_log(message, "ERROR")

    def log_warning(self, message: str, context: str = ""):
        """Log warnings with context"""
        context_str = f" | Context: {context}" if context else ""
        warning_message = f"WARNING: {message}{context_str}"
        self.logger.warning(warning_message)
        print_enhanced_log(warning_message, "WARNING")

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

        message = (
            f"SESSION METRICS | "
            f"Queries: {self.metrics['queries_executed']} | "
            f"Success Rate: {success_rate:.1f}% | "
            f"Avg Response: {avg_response_time:.2f}s | "
            f"AI Tokens: {self.metrics['ai_tokens_used']} | "
            f"Connections: {self.metrics['active_connections']}"
        )
        self.logger.info(message)
        print_enhanced_log(message, "INFO")

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

    def _is_bad_ai_response(self, response: str) -> bool:
        """Check if AI response contains bad patterns - NO TOLERANCE"""
        bad_patterns = [
            "extract_data",  # Non-existent API
            "load_data",  # Non-existent API
            "if_exists",  # Wrong parameter for MongoDB
            "dataframe",  # Wrong approach for NoSQL
            "### erklärung",  # Markdown in code
            "### anmerkungen",  # Markdown in code
            "```python",  # Markdown blocks
            "```",  # Any markdown
        ]

        response_lower = response.lower()
        for pattern in bad_patterns:
            if pattern.lower() in response_lower:
                return True
        return False


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

    # Print startup message (Windows-safe, no emoji)
    print_enhanced_log("Enhanced ETL Logging System initialized!", "SUCCESS")

    return ETLDesignerLogger()


def print_enhanced_log(message: str, level: str = "INFO"):
    """Print enhanced log message to terminal with visual emphasis - Windows-safe!"""
    colors = {
        "INFO": "\033[94m",  # Blue
        "WARNING": "\033[93m",  # Yellow
        "ERROR": "\033[91m",  # Red
        "SUCCESS": "\033[92m",  # Green
        "CRITICAL": "\033[95m",  # Magenta
    }
    reset = "\033[0m"

    color = colors.get(level.upper(), colors["INFO"])

    # Erstelle formatierte Log-Nachricht
    timestamp = time.strftime("%H:%M:%S")
    formatted_message = f"[{timestamp}] [ETL ENHANCED] {message}"

    try:
        # Console output mit Farbe - SOFORT flushen!
        print(f"{color}{formatted_message}{reset}", flush=True)
    except UnicodeEncodeError:
        # Windows console fallback - without colors
        print(formatted_message, flush=True)
    except Exception:
        # Final fallback
        print(f"[{timestamp}] [ETL ENHANCED] Log message (encoding issue)", flush=True)

    # ZUSÄTZLICH: Schreibe IMMER in separate Enhanced-Log-Datei
    try:
        with open("etl_enhanced_terminal.log", "a", encoding="utf-8") as f:
            f.write(f"{formatted_message}\n")
            f.flush()
    except Exception:
        pass  # Silent fallback

    # ZUSÄTZLICH: Schreibe direkt zu sys.stdout um sicherzustellen, dass der Launcher es sieht
    try:
        import sys

        sys.stdout.write(f"{formatted_message}\n")
        sys.stdout.flush()
    except Exception:
        pass  # Silent fallback
