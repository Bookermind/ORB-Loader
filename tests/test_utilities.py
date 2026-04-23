"""
Unit tests for utilities.py module.

Tests cover:
- str_to_bool function with various inputs
- Edge cases and invalid inputs
"""

import pytest
from orchestrator.utils.utilities import str_to_bool


# ============================================================================
# str_to_bool Tests
# ============================================================================

class TestStrToBool:
    """Tests for str_to_bool utility function."""

    def test_str_to_bool_true_variations(self):
        """Test that various 'true' strings return True."""
        true_values = ["true", "TRUE", "True", "TrUe"]
        
        for value in true_values:
            assert str_to_bool(value) is True, f"Failed for '{value}'"

    def test_str_to_bool_one_variations(self):
        """Test that '1' returns True."""
        assert str_to_bool("1") is True
        assert str_to_bool("1") is True  # With whitespace would need strip

    def test_str_to_bool_yes_variations(self):
        """Test that various 'yes' strings return True."""
        yes_values = ["yes", "YES", "Yes", "YeS"]
        
        for value in yes_values:
            assert str_to_bool(value) is True, f"Failed for '{value}'"

    def test_str_to_bool_on_variations(self):
        """Test that various 'on' strings return True."""
        on_values = ["on", "ON", "On", "oN"]
        
        for value in on_values:
            assert str_to_bool(value) is True, f"Failed for '{value}'"

    def test_str_to_bool_enabled_variations(self):
        """Test that various 'enabled' strings return True."""
        enabled_values = ["enabled", "ENABLED", "Enabled", "EnAbLeD"]
        
        for value in enabled_values:
            assert str_to_bool(value) is True, f"Failed for '{value}'"

    def test_str_to_bool_false_variations(self):
        """Test that various 'false' strings return False."""
        false_values = ["false", "FALSE", "False", "FaLsE"]
        
        for value in false_values:
            assert str_to_bool(value) is False, f"Failed for '{value}'"

    def test_str_to_bool_zero_variations(self):
        """Test that '0' returns False."""
        assert str_to_bool("0") is False

    def test_str_to_bool_no_variations(self):
        """Test that various 'no' strings return False."""
        no_values = ["no", "NO", "No", "nO"]
        
        for value in no_values:
            assert str_to_bool(value) is False, f"Failed for '{value}'"

    def test_str_to_bool_off_variations(self):
        """Test that various 'off' strings return False."""
        off_values = ["off", "OFF", "Off", "oFf"]
        
        for value in off_values:
            assert str_to_bool(value) is False, f"Failed for '{value}'"

    def test_str_to_bool_disabled_variations(self):
        """Test that various 'disabled' strings return False."""
        disabled_values = ["disabled", "DISABLED", "Disabled", "DiSaBlEd"]
        
        for value in disabled_values:
            assert str_to_bool(value) is False, f"Failed for '{value}'"

    def test_str_to_bool_empty_string(self):
        """Test that empty string returns False."""
        assert str_to_bool("") is False

    def test_str_to_bool_whitespace(self):
        """Test that whitespace-only string returns False."""
        assert str_to_bool("   ") is False
        assert str_to_bool("\t") is False
        assert str_to_bool("\n") is False

    def test_str_to_bool_random_string(self):
        """Test that random strings return False."""
        random_values = ["maybe", "random", "xyz", "2", "10", "yep", "nope"]
        
        for value in random_values:
            assert str_to_bool(value) is False, f"Failed for '{value}'"

    def test_str_to_bool_with_leading_trailing_spaces(self):
        """Test behavior with leading/trailing spaces (not stripped by function)."""
        # The function doesn't strip, so these should return False
        assert str_to_bool(" true ") is False
        assert str_to_bool(" yes") is False
        assert str_to_bool("on ") is False

    def test_str_to_bool_numeric_strings(self):
        """Test that numeric strings other than '1' return False."""
        assert str_to_bool("2") is False
        assert str_to_bool("10") is False
        assert str_to_bool("-1") is False
        assert str_to_bool("0.5") is False

    def test_str_to_bool_special_characters(self):
        """Test that strings with special characters return False."""
        assert str_to_bool("true!") is False
        assert str_to_bool("yes?") is False
        assert str_to_bool("@#$%") is False

    def test_str_to_bool_mixed_case_edge_cases(self):
        """Test edge cases with mixed case variations."""
        assert str_to_bool("tRuE") is True
        assert str_to_bool("yES") is True
        assert str_to_bool("oN") is True
        assert str_to_bool("eNaBlEd") is True

    def test_str_to_bool_boolean_like_words_not_in_list(self):
        """Test that boolean-like words not in the list return False."""
        assert str_to_bool("y") is False  # Short for yes, but not in list
        assert str_to_bool("n") is False  # Short for no, but not in list
        assert str_to_bool("t") is False  # Short for true, but not in list
        assert str_to_bool("f") is False  # Short for false, but not in list
        assert str_to_bool("ok") is False
        assert str_to_bool("active") is False

    def test_str_to_bool_all_true_values(self):
        """Test all values that should return True in one assertion."""
        true_values = ["true", "1", "yes", "on", "enabled"]
        
        for value in true_values:
            for case_variant in [value, value.upper(), value.capitalize()]:
                assert str_to_bool(case_variant) is True, \
                    f"Expected True for '{case_variant}'"

    def test_str_to_bool_consistency(self):
        """Test that calling the function multiple times gives consistent results."""
        test_values = [
            ("true", True),
            ("false", False),
            ("yes", True),
            ("no", False),
            ("1", True),
            ("0", False),
            ("random", False),
        ]
        
        for value, expected in test_values:
            # Call multiple times
            for _ in range(3):
                assert str_to_bool(value) is expected, \
                    f"Inconsistent result for '{value}'"


# ============================================================================
# Edge Cases and Type Handling
# ============================================================================

class TestStrToBoolEdgeCases:
    """Tests for edge cases and potential error scenarios."""

    def test_str_to_bool_with_none_raises_attribute_error(self):
        """Test that None input raises AttributeError (no .lower() method)."""
        with pytest.raises(AttributeError):
            str_to_bool(None)

    def test_str_to_bool_with_integer_raises_attribute_error(self):
        """Test that integer input raises AttributeError."""
        with pytest.raises(AttributeError):
            str_to_bool(1)

    def test_str_to_bool_with_boolean_raises_attribute_error(self):
        """Test that boolean input raises AttributeError."""
        with pytest.raises(AttributeError):
            str_to_bool(True)

    def test_str_to_bool_with_list_raises_attribute_error(self):
        """Test that list input raises AttributeError."""
        with pytest.raises(AttributeError):
            str_to_bool(["true"])

    def test_str_to_bool_unicode_strings(self):
        """Test that unicode strings work correctly."""
        assert str_to_bool("true") is True  # ASCII
        assert str_to_bool("True") is True  # ASCII
        # Unicode characters (not in true list)
        assert str_to_bool("trüe") is False
        assert str_to_bool("yes™") is False


# ============================================================================
# Documentation and Usage Examples
# ============================================================================

class TestStrToBoolDocumentation:
    """Tests demonstrating common usage patterns."""

    def test_configuration_value_parsing(self):
        """Example: Parsing configuration values from environment variables."""
        # Simulating env vars or config file values
        config_values = {
            "DEBUG_MODE": "true",
            "ENABLE_LOGGING": "yes",
            "USE_CACHE": "on",
            "AUTO_START": "1",
            "VERBOSE": "enabled",
        }
        
        for key, value in config_values.items():
            result = str_to_bool(value)
            assert result is True, f"Config {key}={value} should be True"

    def test_user_input_parsing(self):
        """Example: Parsing user input from CLI or forms."""
        user_inputs = ["YES", "Yes", "y", "Y", "true", "TRUE"]
        
        # Only exact matches work
        assert str_to_bool("YES") is True
        assert str_to_bool("Yes") is True
        assert str_to_bool("true") is True
        assert str_to_bool("TRUE") is True
        
        # Short forms don't work
        assert str_to_bool("y") is False
        assert str_to_bool("Y") is False

    def test_comparison_with_actual_boolean(self):
        """Example: Comparing str_to_bool result with boolean values."""
        config_value = "true"
        
        if str_to_bool(config_value):
            result = "enabled"
        else:
            result = "disabled"
        
        assert result == "enabled"

    def test_ternary_usage(self):
        """Example: Using str_to_bool in ternary expressions."""
        setting = "yes"
        output = "ON" if str_to_bool(setting) else "OFF"
        
        assert output == "ON"