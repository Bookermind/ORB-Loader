"""
Unit tests for companion_tracker.py module.

Tests cover:
- PendingPair dataclass
- CompanionTracker initialization and configuration
- File pairing workflow (data + companion)
- Timeout handling
- Orphaned companion detection
- Thread safety and concurrency
- Key extraction and companion identification
"""

import pytest
import threading
import time
from unittest.mock import MagicMock, call
from freezegun import freeze_time
from datetime import timedelta

from orchestrator.managers.companion_tracker import (
    CompanionTracker,
    PendingPair,
)
from orchestrator.managers.source_identifier import SourceConfig, SourceRegistry


# ============================================================================
# PendingPair Dataclass Tests
# ============================================================================

class TestPendingPair:
    """Tests for PendingPair dataclass."""

    def test_pending_pair_initialization(self, valid_source_dict):
        """Test PendingPair can be initialized with required fields."""
        config = SourceConfig("sapar", valid_source_dict)
        
        pair = PendingPair(
            source_name="sapar",
            key="20260410",
            config=config,
        )
        
        assert pair.source_name == "sapar"
        assert pair.key == "20260410"
        assert pair.config == config
        assert pair.data_path is None
        assert pair.companion_path is None
        assert pair.data_stable is False
        assert pair.companion_stable is False
        assert pair.timeout_timer is None

    def test_pending_pair_with_all_fields(self, valid_source_dict):
        """Test PendingPair with all optional fields populated."""
        config = SourceConfig("sapar", valid_source_dict)
        mock_timer = MagicMock(spec=threading.Timer)
        
        pair = PendingPair(
            source_name="sapar",
            key="20260410",
            config=config,
            data_path="/data/landing/sapar_20260410.csv",
            companion_path="/data/landing/sapar_20260410.trigger.csv",
            data_stable=True,
            companion_stable=True,
            timeout_timer=mock_timer,
        )
        
        assert pair.data_path == "/data/landing/sapar_20260410.csv"
        assert pair.companion_path == "/data/landing/sapar_20260410.trigger.csv"
        assert pair.data_stable is True
        assert pair.companion_stable is True
        assert pair.timeout_timer == mock_timer


# ============================================================================
# CompanionTracker Initialization Tests
# ============================================================================

class TestCompanionTrackerInitialization:
    """Tests for CompanionTracker initialization."""

    def test_initialization_with_file_strategy_sources(self, valid_source_dict):
        """Test CompanionTracker initializes with file strategy sources."""
        config = SourceConfig("sapar", valid_source_dict)
        sources = {"sapar": config}
        registry = SourceRegistry(sources)
        
        on_pair_ready = MagicMock()
        on_timeout = MagicMock()
        
        tracker = CompanionTracker(
            registry=registry,
            on_pair_ready=on_pair_ready,
            on_timeout=on_timeout,
        )
        
        assert tracker.companion_debounce == 3
        assert len(tracker._companion_patterns) == 1
        assert tracker._companion_patterns[0][0] == config

    def test_initialization_with_custom_debounce(self, valid_source_dict):
        """Test CompanionTracker respects custom debounce setting."""
        config = SourceConfig("sapar", valid_source_dict)
        registry = SourceRegistry({"sapar": config})
        
        tracker = CompanionTracker(
            registry=registry,
            on_pair_ready=MagicMock(),
            on_timeout=MagicMock(),
            companion_debounce=10,
        )
        
        assert tracker.companion_debounce == 10

    def test_initialization_with_orphaned_companion_callback(self, valid_source_dict):
        """Test CompanionTracker accepts orphaned companion callback."""
        config = SourceConfig("sapar", valid_source_dict)
        registry = SourceRegistry({"sapar": config})
        on_orphaned = MagicMock()
        
        tracker = CompanionTracker(
            registry=registry,
            on_pair_ready=MagicMock(),
            on_timeout=MagicMock(),
            on_orphaned_companion=on_orphaned,
        )
        
        assert tracker._on_orphaned_companion == on_orphaned

    def test_initialization_skips_footer_strategy_sources(self, footer_validation_source_dict):
        """Test CompanionTracker ignores sources with footer validation strategy."""
        config = SourceConfig("acme", footer_validation_source_dict)
        registry = SourceRegistry({"acme": config})
        
        tracker = CompanionTracker(
            registry=registry,
            on_pair_ready=MagicMock(),
            on_timeout=MagicMock(),
        )
        
        # Footer strategy sources should not be tracked
        assert len(tracker._companion_patterns) == 0
        assert len(tracker._companion_suffixes) == 0

    def test_initialization_with_suffix_strategy(self):
        """Test CompanionTracker handles suffix-based companion matching."""
        config_dict = {
            "metadata": {
                "file_type": "csv",
                "filename_pattern": r"test_\d+\.csv$",
                "timeout_seconds": 100,
            },
            "validation": {
                "strategy": {
                    "type": "file",
                    "suffix": ".done",
                    "key_pattern": r"test_(\d+)",
                },
                "count_pattern": r"COUNT=(\d+)",
                "amount_pattern": r"AMOUNT=(\d+)",
                "amount_column": {"name": "Amount"},
            },
        }
        config = SourceConfig("test", config_dict)
        registry = SourceRegistry({"test": config})
        
        tracker = CompanionTracker(
            registry=registry,
            on_pair_ready=MagicMock(),
            on_timeout=MagicMock(),
        )
        
        assert len(tracker._companion_suffixes) == 1
        assert tracker._companion_suffixes[0][0] == config
        assert tracker._companion_suffixes[0][1] == ".done"


# ============================================================================
# Companion Identification Tests
# ============================================================================

class TestCompanionIdentification:
    """Tests for identify_companion and extract_key methods."""

    def test_identify_companion_with_pattern_match(self, valid_source_dict):
        """Test identify_companion matches companion file by pattern."""
        config = SourceConfig("sapar", valid_source_dict)
        registry = SourceRegistry({"sapar": config})
        tracker = CompanionTracker(
            registry=registry,
            on_pair_ready=MagicMock(),
            on_timeout=MagicMock(),
        )
        
        result = tracker.identify_companion("sapar_20260410.trigger.csv")
        
        assert result is not None
        assert result[0] == config
        assert result[1] == "20260410"  # Extracted key

    def test_identify_companion_no_match(self, valid_source_dict):
        """Test identify_companion returns None for non-matching filename."""
        config = SourceConfig("sapar", valid_source_dict)
        registry = SourceRegistry({"sapar": config})
        tracker = CompanionTracker(
            registry=registry,
            on_pair_ready=MagicMock(),
            on_timeout=MagicMock(),
        )
        
        result = tracker.identify_companion("unknown_file.csv")
        
        assert result is None

    def test_identify_companion_with_suffix(self):
        """Test identify_companion matches companion file by suffix."""
        config_dict = {
            "metadata": {
                "file_type": "csv",
                "filename_pattern": r"test_\d+\.csv$",
                "timeout_seconds": 100,
            },
            "validation": {
                "strategy": {
                    "type": "file",
                    "suffix": ".done",
                    "key_pattern": r"test_(\d+)",
                },
                "count_pattern": r"COUNT=(\d+)",
                "amount_pattern": r"AMOUNT=(\d+)",
                "amount_column": {"name": "Amount"},
            },
        }
        config = SourceConfig("test", config_dict)
        registry = SourceRegistry({"test": config})
        tracker = CompanionTracker(
            registry=registry,
            on_pair_ready=MagicMock(),
            on_timeout=MagicMock(),
        )
        
        result = tracker.identify_companion("test_12345.csv.done")
        
        assert result is not None
        assert result[0] == config
        assert result[1] == "12345"

    def test_identify_companion_suffix_case_insensitive(self):
        """Test identify_companion suffix matching is case-insensitive."""
        config_dict = {
            "metadata": {
                "file_type": "csv",
                "filename_pattern": r"test\.csv$",
                "timeout_seconds": 100,
            },
            "validation": {
                "strategy": {
                    "type": "file",
                    "suffix": ".DONE",
                    "key_pattern": r"test",
                },
                "count_pattern": r"COUNT=(\d+)",
                "amount_pattern": r"AMOUNT=(\d+)",
                "amount_column": {"name": "Amount"},
            },
        }
        config = SourceConfig("test", config_dict)
        registry = SourceRegistry({"test": config})
        tracker = CompanionTracker(
            registry=registry,
            on_pair_ready=MagicMock(),
            on_timeout=MagicMock(),
        )
        
        result = tracker.identify_companion("test.csv.done")
        assert result is not None

    def test_extract_key_success(self, valid_source_dict):
        """Test extract_key extracts key from filename using regex."""
        config = SourceConfig("sapar", valid_source_dict)
        registry = SourceRegistry({"sapar": config})
        tracker = CompanionTracker(
            registry=registry,
            on_pair_ready=MagicMock(),
            on_timeout=MagicMock(),
        )
        
        key = tracker.extract_key("sapar_20260410.trigger.csv", config)
        
        assert key == "20260410"

    def test_extract_key_no_match_returns_none(self, valid_source_dict):
        """Test extract_key returns None when pattern doesn't match."""
        config = SourceConfig("sapar", valid_source_dict)
        registry = SourceRegistry({"sapar": config})
        tracker = CompanionTracker(
            registry=registry,
            on_pair_ready=MagicMock(),
            on_timeout=MagicMock(),
        )
        
        key = tracker.extract_key("nomatch.csv", config)
        
        assert key is None

    def test_extract_key_missing_pattern_logs_error(self, valid_source_dict, caplog):
        """Test extract_key logs error when no key pattern configured."""
        config = SourceConfig("sapar", valid_source_dict)
        registry = SourceRegistry({"sapar": config})
        tracker = CompanionTracker(
            registry=registry,
            on_pair_ready=MagicMock(),
            on_timeout=MagicMock(),
        )
        
        # Create a config without key pattern in tracker's dict
        fake_config = SourceConfig("fake", valid_source_dict)
        
        key = tracker.extract_key("test.csv", fake_config)
        
        assert key is None
        assert "No key pattern found" in caplog.text


# ============================================================================
# File Pairing Workflow Tests
# ============================================================================

class TestFilePairingWorkflow:
    """Tests for the core file pairing workflow."""

    def test_mark_data_stable_creates_pending_pair(self, valid_source_dict):
        """Test mark_data_stable creates a new pending pair."""
        config = SourceConfig("sapar", valid_source_dict)
        registry = SourceRegistry({"sapar": config})
        on_pair_ready = MagicMock()
        
        tracker = CompanionTracker(
            registry=registry,
            on_pair_ready=on_pair_ready,
            on_timeout=MagicMock(),
        )
        
        tracker.mark_data_stable("sapar", "20260410", "/data/sapar_20260410.csv", config)
        
        # Should create pending pair but not signal ready
        assert ("sapar", "20260410") in tracker._pending
        assert tracker._pending[("sapar", "20260410")].data_stable is True
        on_pair_ready.assert_not_called()

    def test_mark_data_stable_starts_timeout_timer(self, valid_source_dict):
        """Test mark_data_stable starts a timeout timer."""
        config = SourceConfig("sapar", valid_source_dict)
        registry = SourceRegistry({"sapar": config})
        
        tracker = CompanionTracker(
            registry=registry,
            on_pair_ready=MagicMock(),
            on_timeout=MagicMock(),
        )
        
        tracker.mark_data_stable("sapar", "20260410", "/data/sapar_20260410.csv", config)
        
        pair = tracker._pending[("sapar", "20260410")]
        assert pair.timeout_timer is not None
        assert pair.timeout_timer.is_alive()
        
        # Cleanup
        pair.timeout_timer.cancel()

    def test_mark_companion_stable_waits_for_data(self, valid_source_dict):
        """Test mark_companion_stable without data file triggers orphaned callback."""
        config = SourceConfig("sapar", valid_source_dict)
        registry = SourceRegistry({"sapar": config})
        on_orphaned = MagicMock()
        
        tracker = CompanionTracker(
            registry=registry,
            on_pair_ready=MagicMock(),
            on_timeout=MagicMock(),
            on_orphaned_companion=on_orphaned,
        )
        
        tracker.mark_companion_stable(
            "sapar", "20260410", "/data/sapar_20260410.trigger.csv", config
        )
        
        # Should trigger orphaned callback
        on_orphaned.assert_called_once_with("/data/sapar_20260410.trigger.csv")

    def test_successful_pairing_signals_ready(self, valid_source_dict):
        """Test that data + companion pairing signals ready callback."""
        config = SourceConfig("sapar", valid_source_dict)
        registry = SourceRegistry({"sapar": config})
        on_pair_ready = MagicMock()
        
        tracker = CompanionTracker(
            registry=registry,
            on_pair_ready=on_pair_ready,
            on_timeout=MagicMock(),
        )
        
        # Mark data stable first
        tracker.mark_data_stable("sapar", "20260410", "/data/sapar_20260410.csv", config)
        on_pair_ready.assert_not_called()
        
        # Mark companion stable - should signal ready
        tracker.mark_companion_stable(
            "sapar", "20260410", "/data/sapar_20260410.trigger.csv", config
        )
        
        on_pair_ready.assert_called_once_with(
            "/data/sapar_20260410.csv",
            "/data/sapar_20260410.trigger.csv",
            config,
        )

    def test_successful_pairing_companion_first(self, valid_source_dict):
        """Test pairing works when companion arrives before data file."""
        config = SourceConfig("sapar", valid_source_dict)
        registry = SourceRegistry({"sapar": config})
        on_pair_ready = MagicMock()
        on_orphaned = MagicMock()
        
        tracker = CompanionTracker(
            registry=registry,
            on_pair_ready=on_pair_ready,
            on_timeout=MagicMock(),
            on_orphaned_companion=on_orphaned,
        )
        
        # Mark companion first - triggers orphaned since no data yet
        tracker.mark_companion_stable(
            "sapar", "20260410", "/data/sapar_20260410.trigger.csv", config
        )
        on_orphaned.assert_called_once()
        
        # Now mark data - won't pair since companion was already orphaned
        tracker.mark_data_stable("sapar", "20260410", "/data/sapar_20260410.csv", config)
        on_pair_ready.assert_not_called()

    def test_pairing_cancels_timeout_timer(self, valid_source_dict):
        """Test that successful pairing cancels the timeout timer."""
        config = SourceConfig("sapar", valid_source_dict)
        registry = SourceRegistry({"sapar": config})
        
        tracker = CompanionTracker(
            registry=registry,
            on_pair_ready=MagicMock(),
            on_timeout=MagicMock(),
        )
        
        # Mark data stable (starts timer)
        tracker.mark_data_stable("sapar", "20260410", "/data/sapar_20260410.csv", config)
        pair_key = ("sapar", "20260410")
        timer = tracker._pending[pair_key].timeout_timer
        
        assert timer.is_alive()
        
        # Mark companion stable (should cancel timer)
        tracker.mark_companion_stable(
            "sapar", "20260410", "/data/sapar_20260410.trigger.csv", config
        )
        
        # Give timer a moment to be cancelled
        time.sleep(0.1)
        assert not timer.is_alive()

    def test_pairing_removes_from_pending(self, valid_source_dict):
        """Test that successful pairing removes entry from _pending dict."""
        config = SourceConfig("sapar", valid_source_dict)
        registry = SourceRegistry({"sapar": config})
        
        tracker = CompanionTracker(
            registry=registry,
            on_pair_ready=MagicMock(),
            on_timeout=MagicMock(),
        )
        
        tracker.mark_data_stable("sapar", "20260410", "/data/sapar_20260410.csv", config)
        assert ("sapar", "20260410") in tracker._pending
        
        tracker.mark_companion_stable(
            "sapar", "20260410", "/data/sapar_20260410.trigger.csv", config
        )
        
        # Should be removed from pending
        assert ("sapar", "20260410") not in tracker._pending


# ============================================================================
# Timeout Handling Tests
# ============================================================================

class TestTimeoutHandling:
    """Tests for timeout behavior."""

    def test_timeout_triggers_callback(self, valid_source_dict):
        """Test that timeout triggers the on_timeout callback."""
        config_dict = valid_source_dict.copy()
        config_dict["metadata"]["timeout_seconds"] = 1  # 1 second timeout
        config = SourceConfig("sapar", config_dict)
        registry = SourceRegistry({"sapar": config})
        on_timeout = MagicMock()
        
        tracker = CompanionTracker(
            registry=registry,
            on_pair_ready=MagicMock(),
            on_timeout=on_timeout,
        )
        
        tracker.mark_data_stable("sapar", "20260410", "/data/sapar_20260410.csv", config)
        
        # Wait for timeout to fire
        time.sleep(1.5)
        
        on_timeout.assert_called_once_with("/data/sapar_20260410.csv", config)

    def test_timeout_removes_from_pending(self, valid_source_dict):
        """Test that timeout removes the pair from _pending dict."""
        config_dict = valid_source_dict.copy()
        config_dict["metadata"]["timeout_seconds"] = 1
        config = SourceConfig("sapar", config_dict)
        registry = SourceRegistry({"sapar": config})
        
        tracker = CompanionTracker(
            registry=registry,
            on_pair_ready=MagicMock(),
            on_timeout=MagicMock(),
        )
        
        tracker.mark_data_stable("sapar", "20260410", "/data/sapar_20260410.csv", config)
        assert ("sapar", "20260410") in tracker._pending
        
        # Wait for timeout
        time.sleep(1.5)
        
        assert ("sapar", "20260410") not in tracker._pending

    def test_timeout_does_not_fire_after_pairing(self, valid_source_dict):
        """Test that timeout doesn't fire if pairing completes first."""
        config_dict = valid_source_dict.copy()
        config_dict["metadata"]["timeout_seconds"] = 2
        config = SourceConfig("sapar", config_dict)
        registry = SourceRegistry({"sapar": config})
        on_timeout = MagicMock()
        
        tracker = CompanionTracker(
            registry=registry,
            on_pair_ready=MagicMock(),
            on_timeout=on_timeout,
        )
        
        tracker.mark_data_stable("sapar", "20260410", "/data/sapar_20260410.csv", config)
        
        # Complete pairing before timeout
        tracker.mark_companion_stable(
            "sapar", "20260410", "/data/sapar_20260410.trigger.csv", config
        )
        
        # Wait past when timeout would have fired
        time.sleep(2.5)
        
        # Timeout should NOT have been called
        on_timeout.assert_not_called()


# ============================================================================
# Thread Safety Tests
# ============================================================================

class TestThreadSafety:
    """Tests for thread safety and concurrent operations."""

    def test_concurrent_mark_data_stable_thread_safe(self, valid_source_dict):
        """Test that concurrent mark_data_stable calls are thread-safe."""
        config = SourceConfig("sapar", valid_source_dict)
        registry = SourceRegistry({"sapar": config})
        
        tracker = CompanionTracker(
            registry=registry,
            on_pair_ready=MagicMock(),
            on_timeout=MagicMock(),
        )
        
        def mark_data(key):
            tracker.mark_data_stable("sapar", key, f"/data/sapar_{key}.csv", config)
        
        # Create multiple threads marking different files
        threads = [
            threading.Thread(target=mark_data, args=(f"2026041{i}",))
            for i in range(10)
        ]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # All 10 should be in pending
        assert len(tracker._pending) == 10
        
        # Cleanup timers
        tracker.shutdown()

    def test_concurrent_pairing_thread_safe(self, valid_source_dict):
        """Test that concurrent pairing operations are thread-safe."""
        config = SourceConfig("sapar", valid_source_dict)
        registry = SourceRegistry({"sapar": config})
        on_pair_ready = MagicMock()
        
        tracker = CompanionTracker(
            registry=registry,
            on_pair_ready=on_pair_ready,
            on_timeout=MagicMock(),
        )
        
        # Pre-populate with data files
        for i in range(5):
            key = f"2026041{i}"
            tracker.mark_data_stable("sapar", key, f"/data/sapar_{key}.csv", config)
        
        def mark_companion(key):
            tracker.mark_companion_stable(
                "sapar", key, f"/data/sapar_{key}.trigger.csv", config
            )
        
        # Mark companions concurrently
        threads = [
            threading.Thread(target=mark_companion, args=(f"2026041{i}",))
            for i in range(5)
        ]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # All should have paired
        assert on_pair_ready.call_count == 5
        assert len(tracker._pending) == 0


# ============================================================================
# Shutdown Tests
# ============================================================================

class TestShutdown:
    """Tests for shutdown and cleanup."""

    def test_shutdown_cancels_all_timers(self, valid_source_dict):
        """Test shutdown cancels all pending timeout timers."""
        config = SourceConfig("sapar", valid_source_dict)
        registry = SourceRegistry({"sapar": config})
        
        tracker = CompanionTracker(
            registry=registry,
            on_pair_ready=MagicMock(),
            on_timeout=MagicMock(),
        )
        
        # Create multiple pending pairs with timers
        for i in range(5):
            key = f"2026041{i}"
            tracker.mark_data_stable("sapar", key, f"/data/sapar_{key}.csv", config)
        
        # Verify timers are running
        assert all(
            pair.timeout_timer.is_alive()
            for pair in tracker._pending.values()
        )
        
        tracker.shutdown()
        
        # All timers should be cancelled
        time.sleep(0.1)  # Give timers time to cancel
        assert len(tracker._pending) == 0

    def test_shutdown_clears_pending_dict(self, valid_source_dict):
        """Test shutdown clears the _pending dictionary."""
        config = SourceConfig("sapar", valid_source_dict)
        registry = SourceRegistry({"sapar": config})
        
        tracker = CompanionTracker(
            registry=registry,
            on_pair_ready=MagicMock(),
            on_timeout=MagicMock(),
        )
        
        tracker.mark_data_stable("sapar", "20260410", "/data/sapar_20260410.csv", config)
        assert len(tracker._pending) > 0
        
        tracker.shutdown()
        
        assert len(tracker._pending) == 0


# ============================================================================
# Error Handling Tests
# ============================================================================

class TestErrorHandling:
    """Tests for error handling in callbacks."""

    def test_pair_ready_callback_exception_logged(self, valid_source_dict, caplog):
        """Test that exceptions in on_pair_ready callback are logged."""
        config = SourceConfig("sapar", valid_source_dict)
        registry = SourceRegistry({"sapar": config})
        on_pair_ready = MagicMock(side_effect=Exception("Callback error"))
        
        tracker = CompanionTracker(
            registry=registry,
            on_pair_ready=on_pair_ready,
            on_timeout=MagicMock(),
        )
        
        tracker.mark_data_stable("sapar", "20260410", "/data/sapar_20260410.csv", config)
        tracker.mark_companion_stable(
            "sapar", "20260410", "/data/sapar_20260410.trigger.csv", config
        )
        
        assert "Error in pair_ready callback" in caplog.text

    def test_timeout_callback_exception_logged(self, valid_source_dict, caplog):
        """Test that exceptions in on_timeout callback are logged."""
        config_dict = valid_source_dict.copy()
        config_dict["metadata"]["timeout_seconds"] = 1
        config = SourceConfig("sapar", config_dict)
        registry = SourceRegistry({"sapar": config})
        on_timeout = MagicMock(side_effect=Exception("Timeout error"))
        
        tracker = CompanionTracker(
            registry=registry,
            on_pair_ready=MagicMock(),
            on_timeout=on_timeout,
        )
        
        tracker.mark_data_stable("sapar", "20260410", "/data/sapar_20260410.csv", config)
        
        # Wait for timeout
        time.sleep(1.5)
        
        assert "Error in timeout callback" in caplog.text


# ============================================================================
# Multiple Sources Tests
# ============================================================================

class TestMultipleSources:
    """Tests for tracking multiple sources simultaneously."""

    def test_different_sources_tracked_separately(self, multiple_sources_dict):
        """Test that different sources are tracked independently."""
        sources = {
            name: SourceConfig(name, config)
            for name, config in multiple_sources_dict.items()
        }
        # Only keep file strategy sources
        file_sources = {
            name: config
            for name, config in sources.items()
            if config.validation_strategy == "file"
        }
        registry = SourceRegistry(file_sources)
        on_pair_ready = MagicMock()
        
        tracker = CompanionTracker(
            registry=registry,
            on_pair_ready=on_pair_ready,
            on_timeout=MagicMock(),
        )
        
        # Mark data for invoice source
        invoice_config = sources["invoice"]
        tracker.mark_data_stable(
            "invoice", "2026_04", "/data/INV_2026_04.csv", invoice_config
        )
        
        assert ("invoice", "2026_04") in tracker._pending
        assert len(tracker._pending) == 1

    def test_same_key_different_sources_tracked_separately(self, multiple_sources_dict):
        """Test that same key across different sources doesn't conflict."""
        sources = {
            name: SourceConfig(name, config)
            for name, config in multiple_sources_dict.items()
        }
        file_sources = {
            name: config
            for name, config in sources.items()
            if config.validation_strategy == "file"
        }
        registry = SourceRegistry(file_sources)
        
        tracker = CompanionTracker(
            registry=registry,
            on_pair_ready=MagicMock(),
            on_timeout=MagicMock(),
        )
        
        # Both sources could theoretically have same key
        invoice_config = sources["invoice"]
        tracker.mark_data_stable(
            "invoice", "2026_04", "/data/INV_2026_04.csv", invoice_config
        )
        
        # The pair keys are (source_name, key) tuples, so no conflict
        assert ("invoice", "2026_04") in tracker._pending
        
        # Cleanup
        tracker.shutdown()