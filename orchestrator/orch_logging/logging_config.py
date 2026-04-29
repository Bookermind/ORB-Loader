import logging
import time
from pathlib import Path
from typing import Optional

import mssql_python
from utils.utilities import get_db_connection


class SQLServerHandler(logging.Handler):
    """
    Custom SQL logging handler
    """

    def __init__(
        self,
        connection_string: str,
        table_name: str = "admin.OrchestratorLogs",
        max_retries: int = 3,
    ):
        super().__init__(
            level=logging.WARNING
        )  # TODO: Does this need moving to CRITICAL?
        self.connection_string = connection_string
        self.table_name = table_name
        self.max_retries = max_retries
        self._connection: Optional[mssql_python.Connection] = None
        self._consecutive_failures = 0
        self._last_failure_time = 0.0

    def _get_connection(self) -> mssql_python.Connection:
        """
        Get or create a database connection
        """
        if self._connection is None:
            self._connection = get_db_connection(self.connection_string)
        return self._connection

    def _close_connection(self) -> None:
        """
        Close the database connection
        """
        if self._connection is not None:
            try:
                self._connection.close()
            except Exception:
                pass
            finally:
                self._connection = None

    def emit(self, record: logging.LogRecord) -> None:
        """
        Emit a log record to the SQL database
        Falls back gracefully to other log handlers if the database connection is not available
        """
        if self._consecutive_failures >= 10:
            if time.time() - self._last_failure_time < 60:
                # We've had too many failures, skip database logging for 60 seconds
                return

        for attempt in range(self.max_retries):
            try:
                connection = self._get_connection()
                cursor = connection.cursor()
                log_message = self.format(record)
                insert_query = f"""
                    INSERT INTO {self.table_name}
                    (Timestamp, LogLevel, LoggerName, Message, Module, FunctionName, LineNumber)
                    VALUES (GETDATE(),?,?,?,?,?,?)
                """
                cursor.execute(
                    insert_query,
                    (
                        record.levelname,
                        record.name,
                        log_message,
                        record.module,
                        record.funcName,
                        record.lineno,
                    ),
                )
                connection.commit()
                cursor.close()
                self._consecutive_failures = 0
                return
            except Exception:
                self._close_connection()
                if attempt < self.max_retries - 1:
                    wait_time = 2**attempt
                    time.sleep(wait_time)
                else:
                    self._consecutive_failures += 1
                    self._last_failure_time = time.time()
                    self.handleError(record)

    def close(self) -> None:
        """
        Clean up database connection
        """
        self._close_connection()
        super().close()


def setup_logging(
    log_folder: Path,
    log_file_name: str = "orchestrator.log",
    console_level: int = logging.INFO,
    file_level: int = logging.INFO,
) -> logging.Logger:
    """
    Configure centralized logging with file and stream handlers.
    """
    log_folder.mkdir(parents=True, exist_ok=True)
    log_file_path = log_folder / log_file_name

    # Create log formatters
    detailed_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # Root logger settings
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)  # Capture all logs, handlers will filter levels
    # Remove any existing Handlers to avoid confusion
    root_logger.handlers.clear()

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_level)
    console_handler.setFormatter(detailed_formatter)
    root_logger.addHandler(console_handler)
    # File handler
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(file_level)
    file_handler.setFormatter(detailed_formatter)
    root_logger.addHandler(file_handler)

    return root_logger


def add_sql_handler(connection_string: str) -> None:
    """
    Add a SQL Handler to the root logger using a pre-built connection string
    """
    root_logger = logging.getLogger()
    formatter = (
        root_logger.handlers[0].formatter
        if root_logger.handlers
        else logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    sql_handler = SQLServerHandler(connection_string)
    sql_handler.setFormatter(formatter)
    root_logger.addHandler(sql_handler)
    root_logger.info("SQL logging enabled for WARNING level and above")
