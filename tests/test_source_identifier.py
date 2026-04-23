"""
Unit tests for source_identifier.py module.

Tests cover:
- SourceConfig validation and property access
- SourceRegistry pattern matching
- YAML configuration loading
- Helper methods (get_amount_column_index, is_control_file, etc.)
"""

import pytest
import re
import yaml
from pathlib import Path
from typing import Dict, Any
from unittest.mock import mock_open, patch
from orchestrator.managers.source_identifier import (
    SourceConfig,
    SourceRegistry,
    load_sources,
)

# ============================================================================
# SourceConfig Validation Tests
# ============================================================================

class TestSourceConfigValidation:
    """
    Tests for SourceConfig validation logic.
    """
    def test_valid_file_strategy_initialisation(self, valid_source_dict):
        """
        Test that a valid file strategy source config initialises correctly.
        """
        config = SourceConfig("sapar", valid_source_dict)

        assert config.name == "sapar"
        assert config.file_type == "csv"
        assert config.validation_strategy == "file"
        assert config.filename_pattern == r"^sapar_\d{8}\.csv$"

    def test_valid_footer_strategy_initialization(self, footer_validation_source_dict):
        """Test that a valid footer strategy source config initializes successfully."""
        config = SourceConfig("acme", footer_validation_source_dict)
        
        assert config.name == "acme"
        assert config.validation_strategy == "footer"
        assert config.count_pattern == r"RECORD COUNT=(\d+)"
        assert config.amount_pattern == r"TOTAL AMOUNT=(\d+\.\d{2})"

    def test_minimal_valid_config(self, minimal_source_dict):
        """Test that a minimal config with only required fields works."""
        config = SourceConfig("minimal", minimal_source_dict)
        
        assert config.file_type == "csv"
        assert config.validation_strategy == "footer"
        # Should use defaults for optional fields
        assert config.encoding == "utf-8-sig"  # default
        assert config.timeout_seconds == 3000  # default

    def test_missing_file_type_raises_error(self, valid_source_dict):
        """Test that missing metadata.file_type raises ValidationError."""
        del valid_source_dict["metadata"]["file_type"]
        
        with pytest.raises(ValueError, match="file_type is required"):
            SourceConfig("test", valid_source_dict)

    def test_missing_filename_pattern_raises_error(self, valid_source_dict):
        """Test that missing metadata.filename_pattern raises ValidationError."""
        del valid_source_dict["metadata"]["filename_pattern"]
        
        with pytest.raises(ValueError, match="filename_pattern is required"):
            SourceConfig("test", valid_source_dict)

    def test_missing_validation_strategy_raises_error(self, valid_source_dict):
        """Test that missing validation.strategy.type raises ValidationError."""
        del valid_source_dict["validation"]["strategy"]["type"]
        
        with pytest.raises(ValueError, match="validation.strategy.type is required"):
            SourceConfig("test", valid_source_dict)

    def test_footer_strategy_missing_count_pattern_raises_error(self, footer_validation_source_dict):
        """Test that footer strategy without count_pattern raises error."""
        del footer_validation_source_dict["validation"]["count_pattern"]
        
        with pytest.raises(ValueError, match="has not defined validation.count_pattern"):
            SourceConfig("test", footer_validation_source_dict)

    def test_footer_strategy_missing_amount_pattern_raises_error(self, footer_validation_source_dict):
        """Test that footer strategy without amount_pattern raises error."""
        del footer_validation_source_dict["validation"]["amount_pattern"]
        
        with pytest.raises(ValueError, match="has not defined validation.amount_pattern"):
            SourceConfig("test", footer_validation_source_dict)

    def test_file_strategy_missing_pattern_and_suffix_raises_error(self, valid_source_dict):
        """Test that file strategy without pattern or suffix raises error."""
        del valid_source_dict["validation"]["strategy"]["pattern"]
        # No suffix either
        
        with pytest.raises(ValueError, match="must therefore define either a validation.strategy.pattern or a validation.strategy.suffix"):
            SourceConfig("test", valid_source_dict)

    def test_file_strategy_missing_key_pattern_raises_error(self, valid_source_dict):
        """Test that file strategy without key_pattern raises error."""
        del valid_source_dict["validation"]["strategy"]["key_pattern"]
        
        with pytest.raises(ValueError, match="must therefore define a validation.strategy.key_pattern"):
            SourceConfig("test", valid_source_dict)

    def test_missing_amount_column_name_and_position_raises_error(self, valid_source_dict):
        """Test that missing both amount_column.name and position raises error."""
        del valid_source_dict["validation"]["amount_column"]
        
        with pytest.raises(ValueError, match="must define a validation.amount_column.name or a validation.amount_column.position"):
            SourceConfig("test", valid_source_dict)

# ============================================================================
# SourceConfig Property Access Tests
# ============================================================================

class TestSourceConfigProperties:
    """Tests for SourceConfig property access."""

    def test_metadata_properties(self, valid_source_dict):
        """Test all metadata properties return correct values."""
        config = SourceConfig("sapar", valid_source_dict)
        
        assert config.file_type == "csv"
        assert config.encoding == "utf-8-sig"
        assert config.filename_pattern == r"sapar_\d{8}\.csv$"
        assert config.timeout_seconds == 300
        assert config.stable_seconds == 3

    def test_validation_properties_file_strategy(self, valid_source_dict):
        """Test validation properties for file strategy."""
        config = SourceConfig("sapar", valid_source_dict)
        
        assert config.validation_strategy == "file"
        assert config.validation_file_pattern == r"sapar_\d{8}\.trigger\.csv$"
        assert config.validation_key_pattern == r"(\d{8})"
        assert config.validation_file_suffix is None
        assert config.count_pattern == r"Count:\s*(\d+)"
        assert config.amount_pattern == r"Amount:\s*([\$£€]?[\d,]+\.?\d*)"

    def test_validation_properties_footer_strategy(self, footer_validation_source_dict):
        """Test validation properties for footer strategy."""
        config = SourceConfig("acme", footer_validation_source_dict)
        
        assert config.validation_strategy == "footer"
        assert config.validation_file_pattern is None
        assert config.validation_key_pattern is None
        assert config.count_pattern == r"RECORD COUNT=(\d+)"

    def test_amount_column_name(self, valid_source_dict):
        """Test amount_column.name property."""
        config = SourceConfig("sapar", valid_source_dict)
        assert config.amount_column_name == "Amount"
        assert config.amount_column_position is None

    def test_amount_column_position(self, footer_validation_source_dict):
        """Test amount_column.position property."""
        config = SourceConfig("acme", footer_validation_source_dict)
        assert config.amount_column_position == 3
        assert config.amount_column_name is None

    def test_padding_properties(self, valid_source_dict):
        """Test padding properties."""
        config = SourceConfig("sapar", valid_source_dict)
        assert config.header_size == 1
        assert config.footer_size == 2

    def test_padding_defaults_to_zero(self, minimal_source_dict):
        """Test that padding defaults to 0 when not specified."""
        config = SourceConfig("minimal", minimal_source_dict)
        assert config.header_size == 0
        assert config.footer_size == 0

    def test_default_encoding(self, minimal_source_dict):
        """Test that encoding defaults to utf-8-sig."""
        config = SourceConfig("minimal", minimal_source_dict)
        assert config.encoding == "utf-8-sig"

    def test_default_timeout_and_stable(self, minimal_source_dict):
        """Test default timeout and stable seconds."""
        config = SourceConfig("minimal", minimal_source_dict)
        assert config.timeout_seconds == 3000
        assert config.stable_seconds == 3

# ============================================================================
# SourceConfig Helper Methods Tests
# ============================================================================

class TestSourceConfigHelperMethods:
    """Tests for SourceConfig helper methods."""

    def test_get_amount_column_index_by_name_success(self, valid_source_dict):
        """Test get_amount_column_index finds column by name."""
        config = SourceConfig("sapar", valid_source_dict)
        header = ["Date", "Customer", "Amount", "Status"]
        
        index = config.get_amount_column_index(header)
        assert index == 2  # 0-based index

    def test_get_amount_column_index_by_name_not_found(self, valid_source_dict):
        """Test get_amount_column_index raises error when column name not in header."""
        config = SourceConfig("sapar", valid_source_dict)
        header = ["Date", "Customer", "Total", "Status"]  # No "Amount"
        
        with pytest.raises(ValueError, match="Amount Column 'Amount' not found"):
            config.get_amount_column_index(header)

    def test_get_amount_column_index_by_position(self, footer_validation_source_dict):
        """Test get_amount_column_index uses position (1-based -> 0-based)."""
        config = SourceConfig("acme", footer_validation_source_dict)
        header = ["Date", "Customer", "Amount", "Status"]
        
        index = config.get_amount_column_index(header)
        assert index == 2  # Position 3 in YAML -> index 2 in Python

    def test_get_amount_column_index_prefers_name_over_position(self):
        """Test that name is preferred when both name and position are present."""
        config_dict = {
            "metadata": {
                "file_type": "csv",
                "filename_pattern": r"test\.csv$",
            },
            "validation": {
                "strategy": {"type": "footer"},
                "count_pattern": r"COUNT=(\d+)",
                "amount_pattern": r"AMOUNT=(\d+)",
                "amount_column": {
                    "name": "Amount",
                    "position": 5,  # Should be ignored
                },
            },
        }
        config = SourceConfig("test", config_dict)
        header = ["Id", "Amount", "Total"]
        
        index = config.get_amount_column_index(header)
        assert index == 1  # Found "Amount" by name, not position 5

    def test_is_control_file_with_pattern_match(self, valid_source_dict):
        """Test is_control_file returns True for matching pattern."""
        config = SourceConfig("sapar", valid_source_dict)
        
        assert config.is_control_file("sapar_20260410.trigger.csv") is True

    def test_is_control_file_with_pattern_no_match(self, valid_source_dict):
        """Test is_control_file returns False for non-matching pattern."""
        config = SourceConfig("sapar", valid_source_dict)
        
        assert config.is_control_file("sapar_20260410.csv") is False
        assert config.is_control_file("other_file.trigger.csv") is False

    def test_is_control_file_with_suffix(self):
        """Test is_control_file with suffix instead of pattern."""
        config_dict = {
            "metadata": {
                "file_type": "csv",
                "filename_pattern": r"test_\d+\.csv$",
            },
            "validation": {
                "strategy": {
                    "type": "file",
                    "suffix": ".done",
                    "key_pattern": r"(\d+)",
                },
                "count_pattern": r"COUNT=(\d+)",
                "amount_pattern": r"AMOUNT=(\d+)",
                "amount_column": {"name": "Amount"},
            },
        }
        config = SourceConfig("test", config_dict)
        
        assert config.is_control_file("test_123.csv.done") is True
        assert config.is_control_file("test_123.csv") is False

    def test_is_control_file_suffix_case_insensitive(self):
        """Test is_control_file suffix matching is case-insensitive."""
        config_dict = {
            "metadata": {
                "file_type": "csv",
                "filename_pattern": r"test\.csv$",
            },
            "validation": {
                "strategy": {
                    "type": "file",
                    "suffix": ".DONE",
                    "key_pattern": r"(\d+)",
                },
                "count_pattern": r"COUNT=(\d+)",
                "amount_pattern": r"AMOUNT=(\d+)",
                "amount_column": {"name": "Amount"},
            },
        }
        config = SourceConfig("test", config_dict)
        
        assert config.is_control_file("test.csv.done") is True
        assert config.is_control_file("test.csv.DONE") is True

    def test_get_data_filename_from_control_with_suffix(self):
        """Test extracting data filename from control file with suffix."""
        config_dict = {
            "metadata": {
                "file_type": "csv",
                "filename_pattern": r"test\.csv$",
            },
            "validation": {
                "strategy": {
                    "type": "file",
                    "suffix": ".done",
                    "key_pattern": r"(\d+)",
                },
                "count_pattern": r"COUNT=(\d+)",
                "amount_pattern": r"AMOUNT=(\d+)",
                "amount_column": {"name": "Amount"},
            },
        }
        config = SourceConfig("test", config_dict)
        
        data_file = config.get_data_filename_from_control("test_20260410.csv.done")
        assert data_file == "test_20260410.csv"

    def test_get_data_filename_from_control_with_pattern(self, valid_source_dict):
        """Test extracting data filename from control file with pattern."""
        config = SourceConfig("sapar", valid_source_dict)
        
        # With pattern-based matching, just returns the same filename
        data_file = config.get_data_filename_from_control("sapar_20260410.trigger.csv")
        assert data_file == "sapar_20260410.trigger.csv"

# ============================================================================
# SourceRegistry Tests
# ============================================================================

class TestSourceRegistry:
    """Tests for SourceRegistry pattern matching."""

    def test_single_match_returns_config(self, multiple_sources_dict):
        """Test that a single pattern match returns the correct SourceConfig."""
        sources = {
            name: SourceConfig(name, config)
            for name, config in multiple_sources_dict.items()
        }
        registry = SourceRegistry(sources)
        
        result = registry.match("sapar_20260410.csv")
        assert result is not None
        assert result.name == "sapar"

    def test_no_match_returns_none_and_logs_warning(self, multiple_sources_dict, caplog):
        """Test that zero matches returns None and logs warning."""
        sources = {
            name: SourceConfig(name, config)
            for name, config in multiple_sources_dict.items()
        }
        registry = SourceRegistry(sources)
        
        result = registry.match("unknown_file.csv")
        assert result is None
        assert "No source pattern found" in caplog.text
        assert "unknown_file.csv" in caplog.text

    def test_multiple_matches_returns_none_and_logs_error(self, caplog):
        """Test that multiple matches returns None and logs error."""
        # Create two sources with overlapping patterns
        sources_dict = {
            "csv_generic": {
                "metadata": {
                    "file_type": "csv",
                    "filename_pattern": r"\d{8}\.csv$",  # Broad pattern
                },
                "validation": {
                    "strategy": {"type": "footer"},
                    "count_pattern": r"COUNT=(\d+)",
                    "amount_pattern": r"AMOUNT=(\d+)",
                    "amount_column": {"name": "Amount"},
                },
            },
            "csv_specific": {
                "metadata": {
                    "file_type": "csv",
                    "filename_pattern": r"20260410\.csv$",  # Overlapping pattern
                },
                "validation": {
                    "strategy": {"type": "footer"},
                    "count_pattern": r"COUNT=(\d+)",
                    "amount_pattern": r"AMOUNT=(\d+)",
                    "amount_column": {"name": "Amount"},
                },
            },
        }

        sources = {
            name: SourceConfig(name, config)
            for name, config in sources_dict.items()
        }
        registry = SourceRegistry(sources)
        
        result = registry.match("20260410.csv")
        assert result is None
        assert "Ambiguous config" in caplog.text
        assert "matches multiple sources" in caplog.text

    def test_match_uses_fullmatch_not_search(self, valid_source_dict):
        """Test that matching uses fullmatch (anchored), not search (substring)."""
        sources = {"sapar": SourceConfig("sapar", valid_source_dict)}
        registry = SourceRegistry(sources)
        
        # Should match: exact pattern
        assert registry.match("sapar_20260410.csv") is not None
        
        # Should NOT match: has prefix/suffix
        assert registry.match("prefix_sapar_20260410.csv") is None
        assert registry.match("sapar_20260410.csv_suffix") is None

    def test_registry_sources_property(self, multiple_sources_dict):
        """Test that sources property returns the source dictionary."""
        sources = {
            name: SourceConfig(name, config)
            for name, config in multiple_sources_dict.items()
        }
        registry = SourceRegistry(sources)
        
        assert registry.sources == sources
        assert len(registry.sources) == 3
        assert "sapar" in registry.sources

# ============================================================================
# load_sources() Function Tests
# ============================================================================

class TestLoadSources:
    """Tests for load_sources() function and caching behavior."""

    def test_load_sources_from_yaml_file(self, sample_yaml_file):
        """Test loading sources from a YAML file."""
        sources = load_sources(sample_yaml_file)
        
        assert len(sources) == 1
        assert "sapar" in sources
        assert isinstance(sources["sapar"], SourceConfig)
        assert sources["sapar"].file_type == "csv"

    def test_load_sources_caches_result(self, sample_yaml_file, monkeypatch):
        """Test that load_sources caches the result on subsequent calls."""
        # Reset cache first
        import orchestrator.managers.source_identifier as si
        monkeypatch.setattr(si, '_sources_cache', None)
        
        # First call - should load from file
        sources1 = load_sources(sample_yaml_file)
        
        # Second call - should return cached version
        sources2 = load_sources(sample_yaml_file)
        
        assert sources1 is sources2  # Same object reference

    def test_load_sources_creates_source_configs(self, tmp_path):
        """Test that load_sources creates SourceConfig objects for each source."""
        yaml_content = {
            "source1": {
                "metadata": {"file_type": "csv", "filename_pattern": r"s1\.csv$"},
                "validation": {
                    "strategy": {"type": "footer"},
                    "count_pattern": r"C=(\d+)",
                    "amount_pattern": r"A=(\d+)",
                    "amount_column": {"name": "Amt"},
                },
            },
            "source2": {
                "metadata": {"file_type": "txt", "filename_pattern": r"s2\.txt$"},
                "validation": {
                    "strategy": {"type": "footer"},
                    "count_pattern": r"C=(\d+)",
                    "amount_pattern": r"A=(\d+)",
                    "amount_column": {"position": 1},
                },
            },
        }
        
        yaml_path = tmp_path / "test_sources.yaml"
        with open(yaml_path, "w") as f:
            yaml.dump(yaml_content, f)
        
        # Reset cache
        import orchestrator.managers.source_identifier as si
        si._sources_cache = None
        
        sources = load_sources(yaml_path)
        
        assert len(sources) == 2
        assert all(isinstance(config, SourceConfig) for config in sources.values())
        assert sources["source1"].name == "source1"
        assert sources["source2"].name == "source2"

    def test_load_sources_handles_missing_file(self, tmp_path):
        """Test that load_sources raises appropriate error for missing file."""
        missing_path = tmp_path / "nonexistent.yaml"
        
        with pytest.raises(FileNotFoundError):
            load_sources(missing_path)

    def test_load_sources_handles_invalid_yaml(self, tmp_path):
        """Test that load_sources handles malformed YAML."""
        invalid_yaml = tmp_path / "invalid.yaml"
        invalid_yaml.write_text("{ invalid yaml content: [")
        
        with pytest.raises(yaml.YAMLError):
            load_sources(invalid_yaml)

# ============================================================================
# Edge Cases and Integration Tests
# ============================================================================

class TestEdgeCases:
    """Tests for edge cases and unusual scenarios."""

    def test_empty_filename_pattern_still_validates(self):
        """Test behavior with edge case patterns."""
        config_dict = {
            "metadata": {
                "file_type": "csv",
                "filename_pattern": ".*",  # Matches everything
            },
            "validation": {
                "strategy": {"type": "footer"},
                "count_pattern": r"COUNT=(\d+)",
                "amount_pattern": r"AMOUNT=(\d+)",
                "amount_column": {"name": "Amount"},
            },
        }
        config = SourceConfig("test", config_dict)
        
        assert config.filename_pattern == ".*"

    def test_nested_dict_access_with_none_values(self):
        """Test _get_nested handles None values gracefully."""
        config_dict = {
            "metadata": {
                "file_type": "csv",
                "filename_pattern": r"test\.csv$",
                "encoding": None,  # Explicitly None
            },
            "validation": {
                "strategy": {"type": "footer"},
                "count_pattern": r"COUNT=(\d+)",
                "amount_pattern": r"AMOUNT=(\d+)",
                "amount_column": {"name": "Amount"},
            },
        }
        config = SourceConfig("test", config_dict)
        
        # Should return default when value is None
        assert config.encoding == "utf-8-sig"

    def test_amount_column_position_converts_to_int(self):
        """Test that amount_column.position is converted to int."""
        config_dict = {
            "metadata": {
                "file_type": "csv",
                "filename_pattern": r"test\.csv$",
            },
            "validation": {
                "strategy": {"type": "footer"},
                "count_pattern": r"COUNT=(\d+)",
                "amount_pattern": r"AMOUNT=(\d+)",
                "amount_column": {"position": "5"},  # String in YAML
            },
        }
        config = SourceConfig("test", config_dict)
        
        assert config.amount_column_position == 5
        assert isinstance(config.amount_column_position, int)

    def test_padding_size_converts_to_int(self):
        """Test that padding sizes are converted to int."""
        config_dict = {
            "metadata": {
                "file_type": "csv",
                "filename_pattern": r"test\.csv$",
            },
            "validation": {
                "strategy": {"type": "footer"},
                "count_pattern": r"COUNT=(\d+)",
                "amount_pattern": r"AMOUNT=(\d+)",
                "amount_column": {"name": "Amount"},
            },
            "padding": {
                "header_size": "2",  # String
                "footer_size": "3",  # String
            },
        }
        config = SourceConfig("test", config_dict)
        
        assert config.header_size == 2
        assert config.footer_size == 3
        assert isinstance(config.header_size, int)