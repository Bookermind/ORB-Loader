"""
Shared pytest fixtures for ORB-Loader tests.

This module provides reusable test fixtures including:
- Mock YAML configurations
- Temporary directory structures
- Mock loggers
- Sample source configurations
"""

import logging
from pathlib import Path
from typing import Any, Dict
import pytest
from unittest.mock import MagicMock

# ============================================================================
# YAML Configuration Fixtures
# ============================================================================

@pytest.fixture
def valid_source_dict() -> Dict[str, Any]:
    """
    Returns a valid source configuration dictionary matching the real world yaml structure for a datafile with completion strategy - file.
    NOTE: This represents a single source entry from sources.yaml, with required fields.
    """
    return {
        "metadata": {
            "file_type": "csv",
            "encoding": "utf-8-sig",
            "filename_pattern": r"sapar_\d{8}\.csv$",
            "delimiter": ",",
            "timeout_seconds": 300,
            "stable_seconds": 3,
        },
        "validation": {
            "strategy": {
                "type": "file",
                "pattern": r"sapar_\d{8}\.trigger\.csv$",
                "key_pattern": r"(\d{8})",
            },
            "count_pattern": r"Count:\s*(\d+)",
            "amount_pattern": r"Amount:\s*([\$£€]?[\d,]+\.?\d*)",
            "amount_column": {
                "name": "Amount",
            },
        },
        "padding": {
            "header_size": 1,
            "footer_size": 2,
        },
    }

@pytest.fixture
def footer_validation_source_dict() -> Dict[str,Any]:
    """
    Returns a valid source configuration dictionary for a file with completion strategy - footer.
    NOTE: This represents a single source entry from sources.yaml, with required fields.
    """
    return {
        "metadata": {
            "file_type": "csv",
            "encoding": "utf-8",
            "filename_pattern": r"acme_\d{8}\.txt$",
            "delimiter": "|",
            "timeout_seconds": 600,
            "stable_seconds": 5,
        },
        "validation": {
            "strategy": {
                "type": "footer",
            },
            "count_pattern": r"RECORD COUNT=(\d+)",
            "amount_pattern": r"TOTAL AMOUNT=(\d+\.\d{2})",
            "amount_column": {
                "position": 3,  # 1-based position
            },
        },
        "padding": {
            "header_size": 1,
            "footer_size": 1,
        },
    }

@pytest.fixture
def minimal_source_dict() -> Dict[str, Any]:
    """
    Returns a minimal valid source configuration dictionary with only required fields.
    This can be used to test default values and optional fields.
    """
    return {
        "metadata": {
            "file_type": "csv",
            "filename_pattern": r"test_\d+\.csv$",
        },
        "validation": {
            "strategy": {
                "type": "footer",
            },
            "count_pattern": r"COUNT=(\d+)",
            "amount_pattern": r"AMOUNT=(\d+)",
            "amount_column": {
                "name": "TotalAmount",
            },
        },
    }

# ============================================================================
# Directory & File System Fixtures
# ============================================================================

@pytest.fixture
def temp_dir_structure(tmp_path: Path) -> Dict[str, Path]:
    """
    Creates a temporary directory structure mimicking the ORB-Loader folder layout.
    Returns a dictionary with paths for key directories.
    Usage:
        def test_something(temp_dir_structure):
            landing = temp_dir_structure['landing']
            test_file = landing / "sapar_20260101.csv"
            test_file.write_text('test_data')
    """
    structure = {
        "root": tmp_path,
        "config": tmp_path / "config",
        "data": tmp_path / "data",
        "landing": tmp_path / "data" / "landing",
        "input": tmp_path / "data" / "input",
        "quarantine": tmp_path / "data" / "quarantine",
        "quarantine_invalid": tmp_path / "data" / "quarantine" / "invalid",
        "quarantine_unknown": tmp_path / "data" / "quarantine" / "unknown",
        "logs": tmp_path / "logs",
        "temp": tmp_path / "data" / "temp",
    }

    #Create all directories
    for path in structure.values():
        path.mkdir(parents=True, exist_ok=True)

    return structure

@pytest.fixture
def sample_yaml_file(tmp_path: Path, valid_source_dict: Dict[str, Any]) -> Path:
    """
    Creates a sample yaml file with a valid yaml structure.
    Returns the path to the yaml file.
    """
    import yaml

    yaml_path = tmp_path / "sources.yaml"
    yaml_content = {"sapar": valid_source_dict}

    with open(yaml_path, "w") as f:
        yaml.dump(yaml_content, f)
    
    return yaml_path

# ============================================================================
# Logging Fixtures
# ============================================================================

@pytest.fixture
def mock_logger() -> MagicMock:
    """
    Returns a mock logger object for testing logging calls without actual log output.

    Usage:
        def test_logging(mock_logger, monkeypatch):
            monkeypatch.setattr('module.logger', mock_logger)
            # TEST CODE THAT LOGS ......
            mock_logger.error.assert_called_once()
    """
    return MagicMock(spec=logging.Logger)

@pytest.fixture(autouse=True)
def reset_logging():
    """
    Resets logging automatically between test to prevent interferences
    """
    yield
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

# ============================================================================
# Source Registry Fixtures
# ============================================================================

@pytest.fixture
def multiple_sources_dict() -> Dict[str, Dict[str, Any]]:
    """
    Returns multiple source configurations for testing registry matching.
    """
    return {
        "sapar": {
            "metadata": {
                "file_type": "csv",
                "filename_pattern": r"sapar_\d{8}\.csv$",
            },
            "validation": {
                "strategy": {"type": "footer"},
                "count_pattern": r"COUNT=(\d+)",
                "amount_pattern": r"AMOUNT=(\d+)",
                "amount_column": {"name": "Amount"},
            },
        },
        "acme": {
            "metadata": {
                "file_type": "txt",
                "filename_pattern": r"acme_\d{6}\.txt$",
            },
            "validation": {
                "strategy": {"type": "footer"},
                "count_pattern": r"TOTAL=(\d+)",
                "amount_pattern": r"AMOUNT=(\d+)",
                "amount_column": {"position": 2},
            },
        },
        "invoice": {
            "metadata": {
                "file_type": "csv",
                "filename_pattern": r"INV_\d{4}_\d{2}\.csv$",
            },
            "validation": {
                "strategy": {
                    "type": "file",
                    "pattern": r"INV_\d{4}_\d{2}\.trigger$",
                    "key_pattern": r"INV_(\d{4}_\d{2})",
                },
                "count_pattern": r"LINES=(\d+)",
                "amount_pattern": r"TOTAL=(\d+)",
                "amount_column": {"name": "InvoiceAmount"},
            },
        },
    }

# ============================================================================
# Cache Reset Fixtures
# ============================================================================

def reset_sources_cache(monkeypatch):
    """
    Automatically reset the global _sources_cache between tests.
    
    This prevents cache pollution between tests that call load_sources().
    """
    import orchestrator.managers.source_identifier as si
    monkeypatch.setattr(si, '_sources_cache', None)