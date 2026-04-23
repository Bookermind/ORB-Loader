"""
Unit tests for logging_config.py module.

Tests cover:
- SQLServerHandler initialization and configuration
- Database connection handling and retry logic
- emit() method with success and failure scenarios
- setup_logging() function with various configurations
- Error handling and graceful degradation
"""

import pytest
import logging
import time
from pathlib import Path
from unittest.mock import MagicMock, patch, mock_open, call
from orchestrator.orch_logging.logging_config import (
    SQLServerHandler,
    setup_logging,
    add_sql_handler,
)


# ============================================================================
# SQLServerHandler Initialization Tests
# ============================================================================

class TestSQLServerHandlerInitialization:
    """Tests for SQLServerHandler initialization."""

    def test_handler_initialization(self):
        """Test SQLServerHandler initializes with default parameters."""
        handler = SQLServerHandler(
            connection_string="DRIVER={SQL Server};SERVER=localhost;DATABASE=test;"
        )
        
        assert handler.connection_string is not None
        assert handler.table_name == "admin.OrchestratorLogs"
        assert handler.max_retries == 3
        assert handler.level == logging.WARNING
        assert handler._connection is None
        assert handler._consecutive_failures == 0

    def test_handler_initialization_custom_params(self):
        """Test SQLServerHandler with custom parameters."""
        handler = SQLServerHandler(
            connection_string="DRIVER={SQL Server};SERVER=localhost;",
            table_name="custom.Logs",
            max_retries=5,
        )
        
        assert handler.table_name == "custom.Logs"
        assert handler.max_retries == 5


# ============================================================================
# SQLServerHandler Connection Tests
# ============================================================================

class TestSQLServerHandlerConnection:
    """Tests for database connection management."""

    @patch('orchestrator.orch_logging.logging_config.get_db_connection')
    def test_get_connection_creates_new_connection(self, mock_get_db_conn):
        """Test _get_connection creates a new connection when none exists."""
        mock_conn = MagicMock()
        mock_get_db_conn.return_value = mock_conn
        
        handler = SQLServerHandler(connection_string="test_connection_string")
        connection = handler._get_connection()
        
        assert connection == mock_conn
        mock_get_db_conn.assert_called_once_with("test_connection_string")

    @patch('orchestrator.orch_logging.logging_config.get_db_connection')
    def test_get_connection_reuses_existing_connection(self, mock_get_db_conn):
        """Test _get_connection reuses existing connection."""
        mock_conn = MagicMock()
        mock_get_db_conn.return_value = mock_conn
        
        handler = SQLServerHandler(connection_string="test_connection_string")
        
        # First call creates connection
        conn1 = handler._get_connection()
        # Second call should reuse
        conn2 = handler._get_connection()
        
        assert conn1 == conn2
        mock_get_db_conn.assert_called_once()  # Only called once

    @patch('orchestrator.orch_logging.logging_config.get_db_connection')
    def test_close_connection_closes_existing_connection(self, mock_get_db_conn):
        """Test _close_connection properly closes an open connection."""
        mock_conn = MagicMock()
        mock_get_db_conn.return_value = mock_conn
        
        handler = SQLServerHandler(connection_string="test_connection_string")
        handler._get_connection()  # Create connection
        
        handler._close_connection()
        
        mock_conn.close.assert_called_once()
        assert handler._connection is None

    def test_close_connection_when_no_connection_exists(self):
        """Test _close_connection handles None connection gracefully."""
        handler = SQLServerHandler(connection_string="test_connection_string")
        
        # Should not raise error
        handler._close_connection()
        assert handler._connection is None


# ============================================================================
# SQLServerHandler emit() Tests
# ============================================================================

class TestSQLServerHandlerEmit:
    """Tests for emit() method."""

    @patch('orchestrator.orch_logging.logging_config.get_db_connection')
    def test_emit_success(self, mock_get_db_conn):
        """Test emit successfully writes log record to database."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_db_conn.return_value = mock_conn
        
        handler = SQLServerHandler(connection_string="test_connection_string")
        handler.setFormatter(logging.Formatter("%(message)s"))
        
        record = logging.LogRecord(
            name="test_logger",
            level=logging.ERROR,
            pathname="test.py",
            lineno=42,
            msg="Test log message",
            args=(),
            exc_info=None,
            func="test_function",
        )
        
        handler.emit(record)
        
        # Verify SQL execution
        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args[0]
        assert "INSERT INTO" in call_args[0]
        assert call_args[1] == (
            "ERROR",
            "test_logger",
            "Test log message",
            "test",
            "test_function",
            42,
        )
        mock_conn.commit.assert_called_once()
        assert handler._consecutive_failures == 0

    @patch('orchestrator.orch_logging.logging_config.get_db_connection')
    def test_emit_retries_on_failure(self, mock_get_db_conn):
        """Test emit retries on database failure."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.execute.side_effect = Exception("DB error")
        mock_get_db_conn.return_value = mock_conn
        
        handler = SQLServerHandler(connection_string="test_connection_string", max_retries=3)
        handler.setFormatter(logging.Formatter("%(message)s"))
        
        record = logging.LogRecord(
            name="test_logger",
            level=logging.ERROR,
            pathname="test.py",
            lineno=42,
            msg="Test log message",
            args=(),
            exc_info=None,
        )
        
        with patch('time.sleep'):  # Skip sleep delays in test
            handler.emit(record)
        
        # Should have tried 3 times
        assert mock_cursor.execute.call_count == 3
        assert handler._consecutive_failures == 1

    @patch('orchestrator.orch_logging.logging_config.get_db_connection')
    def test_emit_closes_connection_on_retry(self, mock_get_db_conn):
        """Test emit closes connection between retries."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.execute.side_effect = Exception("DB error")
        mock_get_db_conn.return_value = mock_conn
        
        handler = SQLServerHandler(connection_string="test_connection_string", max_retries=2)
        handler.setFormatter(logging.Formatter("%(message)s"))
        
        record = logging.LogRecord(
            name="test_logger",
            level=logging.ERROR,
            pathname="test.py",
            lineno=42,
            msg="Test",
            args=(),
            exc_info=None,
        )
        
        with patch('time.sleep'):
            handler.emit(record)
        
        # Connection should have been closed between retries
        assert mock_conn.close.call_count >= 1

    @patch('orchestrator.orch_logging.logging_config.get_db_connection')
    def test_emit_skips_after_consecutive_failures(self, mock_get_db_conn):
        """Test emit skips logging after 10 consecutive failures."""
        mock_conn = MagicMock()
        mock_get_db_conn.return_value = mock_conn
        mock_conn.cursor.side_effect = Exception("DB error")
        
        handler = SQLServerHandler(connection_string="test_connection_string", max_retries=1)
        handler.setFormatter(logging.Formatter("%(message)s"))
        handler._consecutive_failures = 10
        handler._last_failure_time = time.time()
        
        record = logging.LogRecord(
            name="test_logger",
            level=logging.ERROR,
            pathname="test.py",
            lineno=42,
            msg="Test",
            args=(),
            exc_info=None,
        )
        
        # Should skip emit without attempting connection
        handler.emit(record)
        
        mock_get_db_conn.assert_not_called()

    @patch('orchestrator.orch_logging.logging_config.get_db_connection')
    def test_emit_resumes_after_cooldown_period(self, mock_get_db_conn):
        """Test emit resumes after 60-second cooldown period."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_db_conn.return_value = mock_conn
        
        handler = SQLServerHandler(connection_string="test_connection_string")
        handler.setFormatter(logging.Formatter("%(message)s"))
        handler._consecutive_failures = 10
        handler._last_failure_time = time.time() - 61  # 61 seconds ago
        
        record = logging.LogRecord(
            name="test_logger",
            level=logging.ERROR,
            pathname="test.py",
            lineno=42,
            msg="Test",
            args=(),
            exc_info=None,
        )
        
        handler.emit(record)
        
        # Should attempt connection after cooldown
        mock_get_db_conn.assert_called_once()

    @patch('orchestrator.orch_logging.logging_config.get_db_connection')
    def test_emit_resets_failure_count_on_success(self, mock_get_db_conn):
        """Test emit resets consecutive failure count on successful log."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_db_conn.return_value = mock_conn
        
        handler = SQLServerHandler(connection_string="test_connection_string")
        handler.setFormatter(logging.Formatter("%(message)s"))
        handler._consecutive_failures = 5
        
        record = logging.LogRecord(
            name="test_logger",
            level=logging.ERROR,
            pathname="test.py",
            lineno=42,
            msg="Test",
            args=(),
            exc_info=None,
        )
        
        handler.emit(record)
        
        assert handler._consecutive_failures == 0


# ============================================================================
# SQLServerHandler close() Tests
# ============================================================================

class TestSQLServerHandlerClose:
    """Tests for close() method."""

    @patch('orchestrator.orch_logging.logging_config.get_db_connection')
    def test_close_closes_connection(self, mock_get_db_conn):
        """Test close() closes the database connection."""
        mock_conn = MagicMock()
        mock_get_db_conn.return_value = mock_conn
        
        handler = SQLServerHandler(connection_string="test_connection_string")
        handler._get_connection()  # Create connection
        
        handler.close()
        
        mock_conn.close.assert_called_once()

    def test_close_calls_super(self):
        """Test close() calls parent class close()."""
        handler = SQLServerHandler(connection_string="test_connection_string")
        
        with patch.object(logging.Handler, 'close') as mock_super_close:
            handler.close()
            mock_super_close.assert_called_once()


# ============================================================================
# setup_logging() Function Tests
# ============================================================================

class TestSetupLogging:
    """Tests for setup_logging() function."""

    def test_setup_logging_creates_log_folder(self, tmp_path):
        """Test setup_logging creates log folder if it doesn't exist."""
        log_folder = tmp_path / "logs"
        assert not log_folder.exists()
        
        logger = setup_logging(
            log_folder=log_folder,
        )
        
        assert log_folder.exists()

    def test_setup_logging_creates_file_handler(self, tmp_path):
        """Test setup_logging creates a file handler."""
        log_folder = tmp_path / "logs"
        
        logger = setup_logging(
            log_folder=log_folder,
            log_file_name="test.log",
        )
        
        log_file = log_folder / "test.log"
        assert log_file.exists()

    def test_setup_logging_returns_root_logger(self, tmp_path):
        """Test setup_logging returns the root logger."""
        log_folder = tmp_path / "logs"
        
        logger = setup_logging(log_folder=log_folder)
        
        assert logger == logging.getLogger()

    def test_setup_logging_clears_existing_handlers(self, tmp_path):
        """Test setup_logging clears existing handlers."""
        log_folder = tmp_path / "logs"
        root_logger = logging.getLogger()
        
        # Add a dummy handler
        dummy_handler = logging.NullHandler()
        root_logger.addHandler(dummy_handler)
        initial_count = len(root_logger.handlers)
        
        setup_logging(log_folder=log_folder)
        
        # Old handlers should be cleared
        assert dummy_handler not in root_logger.handlers

    def test_setup_logging_sets_console_handler_level(self, tmp_path):
        """Test setup_logging sets console handler to specified level."""
        log_folder = tmp_path / "logs"
        
        logger = setup_logging(
            log_folder=log_folder,
            console_level=logging.DEBUG,
        )
        
        # Find console handler (StreamHandler)
        console_handlers = [
            h for h in logger.handlers
            if isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)
        ]
        assert len(console_handlers) == 1
        assert console_handlers[0].level == logging.DEBUG

    def test_setup_logging_sets_file_handler_level(self, tmp_path):
        """Test setup_logging sets file handler to specified level."""
        log_folder = tmp_path / "logs"
        
        logger = setup_logging(
            log_folder=log_folder,
            file_level=logging.WARNING,
        )
        
        # Find file handler
        file_handlers = [h for h in logger.handlers if isinstance(h, logging.FileHandler)]
        assert len(file_handlers) == 1
        assert file_handlers[0].level == logging.WARNING

    def test_setup_logging_formatter_applied(self, tmp_path):
        """Test setup_logging applies formatter to all handlers."""
        log_folder = tmp_path / "logs"
        
        logger = setup_logging(log_folder=log_folder)
        
        for handler in logger.handlers:
            assert handler.formatter is not None
            # Check formatter format string contains expected elements
            format_str = handler.formatter._fmt
            assert "%(asctime)s" in format_str
            assert "%(name)s" in format_str
            assert "%(levelname)s" in format_str
            assert "%(message)s" in format_str


# ============================================================================
# add_sql_handler() Tests
# ============================================================================

class TestAddSqlHandler:
    """Tests for add_sql_handler() function."""

    def test_add_sql_handler_adds_handler(self, tmp_path):
        """Test add_sql_handler adds a SQLServerHandler to the root logger."""
        log_folder = tmp_path / "logs"
        setup_logging(log_folder=log_folder)

        add_sql_handler("DRIVER={SQL Server};SERVER=localhost;DATABASE=test;")

        root_logger = logging.getLogger()
        sql_handlers = [h for h in root_logger.handlers if isinstance(h, SQLServerHandler)]
        assert len(sql_handlers) == 1
        assert sql_handlers[0].connection_string == "DRIVER={SQL Server};SERVER=localhost;DATABASE=test;"

    def test_add_sql_handler_uses_existing_formatter(self, tmp_path):
        """Test add_sql_handler picks up the formatter from existing handlers."""
        log_folder = tmp_path / "logs"
        setup_logging(log_folder=log_folder)

        add_sql_handler("DRIVER={SQL Server};SERVER=localhost;DATABASE=test;")

        root_logger = logging.getLogger()
        sql_handlers = [h for h in root_logger.handlers if isinstance(h, SQLServerHandler)]
        assert sql_handlers[0].formatter is not None
        format_str = sql_handlers[0].formatter._fmt
        assert "%(asctime)s" in format_str
        assert "%(levelname)s" in format_str

    def test_add_sql_handler_with_no_existing_handlers(self):
        """Test add_sql_handler creates default formatter when no handlers exist."""
        root_logger = logging.getLogger()
        root_logger.handlers.clear()

        add_sql_handler("DRIVER={SQL Server};SERVER=localhost;DATABASE=test;")

        sql_handlers = [h for h in root_logger.handlers if isinstance(h, SQLServerHandler)]
        assert len(sql_handlers) == 1
        assert sql_handlers[0].formatter is not None


# ============================================================================
# Integration Tests
# ============================================================================

class TestLoggingIntegration:
    """Integration tests for the logging system."""

    def test_logging_to_file_works(self, tmp_path):
        """Test that logging to file actually works end-to-end."""
        log_folder = tmp_path / "logs"
        log_file_name = "test.log"
        
        logger = setup_logging(
            log_folder=log_folder,
            log_file_name=log_file_name,
            file_level=logging.INFO,
        )
        
        test_message = "Test log message for integration test"
        logger.info(test_message)
        
        log_file = log_folder / log_file_name
        content = log_file.read_text()
        assert test_message in content

    def test_logging_respects_levels(self, tmp_path):
        """Test that logging respects configured levels."""
        log_folder = tmp_path / "logs"
        log_file_name = "test.log"
        
        logger = setup_logging(
            log_folder=log_folder,
            log_file_name=log_file_name,
            file_level=logging.WARNING,  # Only WARNING and above
        )
        
        logger.debug("Debug message")
        logger.info("Info message")
        logger.warning("Warning message")
        logger.error("Error message")
        
        log_file = log_folder / log_file_name
        content = log_file.read_text()
        
        assert "Debug message" not in content
        assert "Info message" not in content
        assert "Warning message" in content
        assert "Error message" in content

    def test_multiple_loggers_use_same_handlers(self, tmp_path):
        """Test that multiple loggers share the same root configuration."""
        log_folder = tmp_path / "logs"
        
        setup_logging(log_folder=log_folder)
        
        logger1 = logging.getLogger("module1")
        logger2 = logging.getLogger("module2")
        
        logger1.info("Message from module1")
        logger2.info("Message from module2")
        
        log_file = log_folder / "orchestrator.log"
        content = log_file.read_text()
        
        assert "module1" in content
        assert "module2" in content