import logging
import os
from pathlib import Path

from managers.file_watcher import FileWatcher
from managers.source_identifier import SourceConfig

PROJECT_ROOT = Path(__file__).resolve().parent.parent

LOG_FOLDER = Path(os.getenv("LOG_FOLDER_PATH", Path(PROJECT_ROOT / "logs")))
ORCHESTRATOR_LOG = Path(LOG_FOLDER / "orchestrator.log")

DATA_FOLDER = Path(os.getenv("DATA_FOLDER_PATH", Path(PROJECT_ROOT / "data")))
LANDING_FOLDER = Path(os.getenv("LANDING_FOLDER_PATH", Path(DATA_FOLDER / "landing")))
INPUT_FOLDER = Path(os.getenv("INPUT_FOLDER_PATH", Path(DATA_FOLDER / "input")))
QUARANTINE_FOLDER = Path(
    os.getenv("QUARANTINE_FOLDER_PATH", Path(DATA_FOLDER / "quarantine"))
)
UNKNOWN_FOLDER = Path(
    os.getenv("UNKNOWN_FOLDER_PATH", Path(QUARANTINE_FOLDER / "unknown"))
)

# Ensure directories exist
LOG_FOLDER.mkdir(parents=True, exist_ok=True)
DATA_FOLDER.mkdir(parents=True, exist_ok=True)
LANDING_FOLDER.mkdir(parents=True, exist_ok=True)
INPUT_FOLDER.mkdir(parents=True, exist_ok=True)
QUARANTINE_FOLDER.mkdir(parents=True, exist_ok=True)
UNKNOWN_FOLDER.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler(ORCHESTRATOR_LOG)],
)
logger = logging.getLogger(__name__)


def on_file_stable(
    data_path: str, companion_path: str | None, config: SourceConfig
) -> None:
    logger.info(
        "[PAIR READY] %s (source: %s, companion: %s)",
        data_path,
        config.name,
        companion_path or "none",
    )
    # Files are already in data/input/
    # --- downstream processing here ---


watcher = FileWatcher(
    watch_dir=LANDING_FOLDER,
    callback=on_file_stable,
    input_dir=INPUT_FOLDER,
    quarantine_dir=QUARANTINE_FOLDER,
    unknown_dir=UNKNOWN_FOLDER,
    recursive=False,
)


def main():
    watcher.start()


if __name__ == "__main__":
    main()
