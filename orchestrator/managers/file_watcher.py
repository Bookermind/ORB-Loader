import hashlib
import logging
import threading
from pathlib import Path
from typing import Callable, Optional

from utils.utilities import atomic_move
from watchfiles import Change, watch

from .companion_tracker import CompanionTracker
from .source_identifier import (
    SourceConfig,
    SourceRegistry,
    load_sources,
)

logger = logging.getLogger(__name__)


class DebounceHandler:
    def __init__(
        self,
        callback: Callable[[str, SourceConfig], None],
        fixed_debounce: Optional[int] = None,
    ):
        self._callback = callback
        self._fixed_debounce = fixed_debounce
        self._timers: dict[str, threading.Timer] = {}
        self._lock = threading.Lock()

    def on_event(self, path: str, config: SourceConfig) -> None:
        debounce = (
            self._fixed_debounce
            if self._fixed_debounce is not None
            else config.stable_seconds
        )
        with self._lock:
            if path in self._timers:
                self._timers[path].cancel()
            timer = threading.Timer(debounce, self._on_stable, args=[path, config])
            self._timers[path] = timer
            timer.start()

    def _on_stable(self, path: str, config: SourceConfig) -> None:
        with self._lock:
            self._timers.pop(path, None)
        self._callback(path, config)

    def shutdown(self) -> None:
        with self._lock:
            for timer in self._timers.values():
                timer.cancel()
            self._timers.clear()


class FileWatcher:
    def __init__(
        self,
        watch_dir: Path,
        callback: Callable[[str, Optional[str], SourceConfig], None],
        input_dir: Optional[Path] = None,
        quarantine_dir: Optional[Path] = None,
        unknown_dir: Optional[Path] = None,
        recursive: bool = False,
        force_polling: bool = True,
    ):
        self.registry = SourceRegistry(load_sources())
        self.callback = callback
        self.watch_dir = watch_dir
        self.recursive = recursive
        self.force_polling = force_polling

        self.input_dir = input_dir or watch_dir.parent / "input"
        self.quarantine_dir = quarantine_dir or watch_dir.parent / "quarantine"
        self.unknown_dir = unknown_dir or self.quarantine_dir / "unknown"

        self.companion_tracker = CompanionTracker(
            registry=self.registry,
            on_pair_ready=self._on_pair_ready,
            on_timeout=self._on_timeout,
            on_orphaned_companion=self._on_orphaned_companion,
        )

        self.data_handler = DebounceHandler(self._on_data_stable)
        self.companion_handler = DebounceHandler(
            self._on_companion_stable,
            fixed_debounce=self.companion_tracker.companion_debounce,
        )

    def start(self) -> None:
        self._ensure_dirs()
        logger.info("Watching %s", self.watch_dir)
        try:
            for changes in watch(
                self.watch_dir,
                watch_filter=None,
                debounce=100,
                step=50,
                recursive=self.recursive,
                force_polling=self.force_polling,
            ):
                self._handle_changes(changes)
        except KeyboardInterrupt:
            logger.info("Stopped by user")
        finally:
            self.data_handler.shutdown()
            self.companion_handler.shutdown()
            self.companion_tracker.shutdown()

    def _ensure_dirs(self) -> None:
        for d in (self.input_dir, self.quarantine_dir, self.unknown_dir):
            d.mkdir(parents=True, exist_ok=True)

    def _handle_changes(self, changes: set[tuple[Change, str]]) -> None:
        for change_type, filepath in changes:
            if change_type not in (Change.added, Change.modified):
                continue
            
            logger.debug("Change detected: %s - %s", change_type.name, filepath)
            path = Path(filepath)
            if not path.is_file():
                logger.debug("Skipping non-file change: %s", filepath)
                continue

            filename = path.name
            logger.debug("Processing file: %s", filename)

            config = self.registry.match(filename)
            if config is None:
                logger.debug("No config match for file: %s", filename)
                self._handle_unmatched(filepath, filename)
            else:
                logger.debug("Matched config '%s' for file: %s", config.name, filename)
                self.data_handler.on_event(filepath, config)

    def _handle_unmatched(self, filepath: str, filename: str) -> None:
        result = self.companion_tracker.identify_companion(filename)
        if result:
            comp_config, _key = result
            self.companion_handler.on_event(filepath, comp_config)
        else:
            logger.warning("Unknown file. Moving to quarantine: %s", filepath)
            self._move_to_unknown(filepath)

    def _on_data_stable(self, filepath: str, config: SourceConfig) -> None:
        if config.validation_strategy == "file":
            key = self.companion_tracker.extract_key(Path(filepath).name, config)
            if key is None:
                logger.error("Failed to extract key from data file: %s", filepath)
                self._move_to_quarentine(filepath)
                return
            self.companion_tracker.mark_data_stable(config.name, key, filepath, config)
        else:
            input_path = self._move_to_input(filepath)
            self.callback(input_path, None, config)

    def _on_companion_stable(self, filepath: str, config: SourceConfig) -> None:
        key = self.companion_tracker.extract_key(Path(filepath).name, config)
        if key is None:
            logger.error("Failed to extract key from companion file: %s", filepath)
            self._move_to_quarentine(filepath)
            return
        self.companion_tracker.mark_companion_stable(config.name, key, filepath, config)

    def _on_pair_ready(
        self, data_path: str, companion_path: Optional[str], config: SourceConfig
    ) -> None:
        input_data_path = self._move_to_input(data_path)
        input_companion_path = self._move_to_input(companion_path) if companion_path else None
        self.callback(input_data_path, input_companion_path, config)

    def _on_timeout(self, data_path: str, config: SourceConfig) -> None:
        logger.warning("Companion timeout (%ds): %s", config.timeout_seconds, data_path)
        self._move_to_quarentine(data_path)

    def _on_orphaned_companion(self, companion_path: str) -> None:
        logger.warning("Orphaned companion file: %s", companion_path)
        self._move_to_quarentine(companion_path)

    def _move_to_input(self, filepath: str) -> str:
        dest = self.input_dir / Path(filepath).name
        logger.info("Moving file to input: %s -> %s", filepath, dest)
        result = atomic_move(Path(filepath), Path(dest), True)
        return str(result)

    def _move_to_quarentine(self, filepath: str) -> None:
        dest = self.quarantine_dir / Path(filepath).name
        logger.info("Moving file to quarentine: %s -> %s", filepath, dest)
        atomic_move(Path(filepath), Path(dest), True)

    def _move_to_unknown(self, filepath: str) -> None:
        dest = self.unknown_dir / Path(filepath).name
        logger.info("Moving file to unknown folder: %s -> %s", filepath, dest)
        atomic_move(Path(filepath), Path(dest), True)
