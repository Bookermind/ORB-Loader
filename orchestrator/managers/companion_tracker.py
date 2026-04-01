import logging
import re
import threading
from dataclasses import dataclass
from typing import Callable, Optional

from .source_identifier import SourceConfig, SourceRegistry

logger = logging.getLogger(__name__)


@dataclass
class PendingPair:
    source_name: str
    key: str
    config: SourceConfig
    data_path: Optional[str] = None
    companion_path: Optional[str] = None
    data_stable: bool = False
    companion_stable: bool = False
    timeout_timer: Optional[threading.Timer] = None


class CompanionTracker:
    def __init__(
        self,
        registry: SourceRegistry,
        on_pair_ready: Callable[[str, Optional[str], SourceConfig], None],
        on_timeout: Callable[[str, SourceConfig], None],
        companion_debounce: int = 3,
        on_orphaned_companion: Optional[Callable[[str], None]] = None,
    ):
        self._companion_debounce = companion_debounce
        self._on_pair_ready = on_pair_ready
        self._on_timeout = on_timeout
        self._on_orphaned_companion = on_orphaned_companion
        self._pending: dict[tuple[str, str], PendingPair] = {}
        self._lock = threading.Lock()
        self._companion_patterns: list[tuple[SourceConfig, re.Pattern]] = []
        self._companion_suffixes: list[tuple[SourceConfig, str]] = []
        self._key_patterns: dict[str, re.Pattern] = {}

        for config in registry.sources.values():
            if config.validation_strategy == "file":
                if config.validation_file_pattern:
                    self._companion_patterns.append(
                        (config, re.compile(config.validation_file_pattern))
                    )
                if config.validation_file_suffix:
                    self._companion_suffixes.append(
                        (config, config.validation_file_suffix)
                    )
                if config.validation_key_pattern:
                    self._key_patterns[config.name] = re.compile(
                        config.validation_key_pattern
                    )

    def identify_companion(self, filename: str) -> Optional[tuple[SourceConfig, str]]:
        """
        Match a filename against a given companion pattern.
        Returns (config, key) is matched, None is unknown
        """
        for config, pattern in self._companion_patterns:
            if pattern.fullmatch(filename):
                key = self.extract_key(filename, config)
                if key:
                    return config, key

        for config, suffix in self._companion_suffixes:
            if filename.lower().endswith(suffix.lower()):
                key = self.extract_key(filename, config)
                if key:
                    return config, key
        return None

    def extract_key(self, filename: str, config: SourceConfig) -> Optional[str]:
        """
        Extract the shared key from a filename using the source's validation_key_pattern
        """
        key_re = self._key_patterns.get(config.name)
        if not key_re:
            logger.error("No key pattern found for source %s", config.name)
            return None
        match = key_re.search(filename)
        return match.group(1) if match else None

    def mark_data_stable(
        self, source_name: str, key: str, data_path: str, config: SourceConfig
    ) -> None:
        """
        Datafile has become stable. Register it an start a timeout timer
        """
        pair_to_signal = None
        with self._lock:
            pair_key = (source_name, key)
            if pair_key not in self._pending:
                self._pending[pair_key] = PendingPair(
                    source_name=source_name, key=key, config=config
                )
            pair = self._pending[pair_key]
            pair.data_path = data_path
            pair.data_stable = True

            if pair.companion_stable:
                pair_to_signal = self._pending.pop(pair_key)
                if pair.timeout_timer:
                    pair.timeout_timer.cancel()
            elif pair.timeout_timer is None:
                pair.timeout_timer = threading.Timer(
                    config.timeout_seconds,
                    self._on_timeout_callback,
                    args=[source_name, key],
                )
                pair.timeout_timer.start()
        if pair_to_signal:
            self._signal_ready(pair_to_signal)

    def mark_companion_stable(
        self, source_name: str, key: str, companion_path: str, config: SourceConfig
    ) -> None:
        """
        Companion file has become stable. Check if it's datafile is also ready
        """
        pair_to_signal = None
        orphaned = False
        with self._lock:
            pair_key = (source_name, key)
            if pair_key not in self._pending:
                orphaned = True
            else:
                pair = self._pending[pair_key]
                pair.companion_path = companion_path
                pair.companion_stable = True

                if pair.data_stable:
                    pair_to_signal = self._pending.pop(pair_key)
                    if pair.timeout_timer:
                        pair.timeout_timer.cancel()

        if pair_to_signal:
            self._signal_ready(pair_to_signal)
        elif orphaned and self._on_orphaned_companion:
            self._on_orphaned_companion(companion_path)

    def shutdown(self) -> None:
        """
        Cancel all pending timeout timers
        """
        with self._lock:
            for pair in self._pending.values():
                if pair.timeout_timer:
                    pair.timeout_timer.cancel()
            self._pending.clear()

    def _signal_ready(self, pair: PendingPair) -> None:
        if self._on_pair_ready:
            assert pair.data_path is not None
            try:
                self._on_pair_ready(pair.data_path, pair.companion_path, pair.config)
            except Exception as e:
                logger.error("Error in pair_ready callback: %s", e)

    def _on_timeout_callback(self, source_name: str, key: str) -> None:
        pair_to_signal = None
        with self._lock:
            pair_to_signal = self._pending.pop((source_name, key), None)
        if pair_to_signal and self._on_timeout:
            try:
                assert pair_to_signal.data_path is not None
                self._on_timeout(pair_to_signal.data_path, pair_to_signal.config)
            except Exception as e:
                logger.error("Error in timeout callback: %s", e)

    @property
    def companion_debounce(self) -> int:
        return self._companion_debounce
