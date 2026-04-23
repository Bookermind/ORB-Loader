import datetime
import logging
import os
import re
import sys
from pathlib import Path

from managers.file_watcher import FileWatcher
from managers.source_identifier import SourceConfig
from orch_logging.logging_config import setup_logging, add_sql_handler
from managers.watchdog import create_heartbeat
from utils.utilities import str_to_bool, atomic_move, build_connection_string, get_db_connection, generate_file_hash

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
from loader.main import run as run_loader

HEARTBEAT_ENABLED = str_to_bool(os.getenv("HEARTBEAT", "False"))
MSSQL_ENABLED = str_to_bool(os.getenv("MSSQL_ENABLED", "False"))

LOG_FOLDER = Path(os.getenv("LOG_FOLDER_PATH", Path(PROJECT_ROOT / "logs")))
ORCHESTRATOR_LOG = Path(LOG_FOLDER / "orchestrator.log")

DATA_FOLDER = Path(os.getenv("DATA_FOLDER_PATH", Path(PROJECT_ROOT / "data")))
LANDING_FOLDER = Path(os.getenv("LANDING_FOLDER_PATH", Path(DATA_FOLDER / "landing")))
INPUT_FOLDER = Path(os.getenv("INPUT_FOLDER_PATH", Path(DATA_FOLDER / "input")))
PROCESSED_FOLDER = Path(
    os.getenv("PROCESSED_FOLDER_PATH", Path(DATA_FOLDER / "processed"))
)
QUARANTINE_FOLDER = Path(
    os.getenv("QUARANTINE_FOLDER_PATH", Path(DATA_FOLDER / "quarantine"))
)
INVALID_FOLDER = Path(
    os.getenv("INVALID_FOLDER_PATH", Path(QUARANTINE_FOLDER / "invalid"))
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
INVALID_FOLDER.mkdir(parents=True, exist_ok=True)
UNKNOWN_FOLDER.mkdir(parents=True, exist_ok=True)
PROCESSED_FOLDER.mkdir(parents=True, exist_ok=True)

# Environment variable based logging levels
console_log_level_str = os.getenv("CONSOLE_LOG_LEVEL", "DEBUG").upper()
file_log_level_str = os.getenv("FILE_LOG_LEVEL", "INFO").upper()

console_log_level = getattr(logging, console_log_level_str, logging.DEBUG)
file_log_level = getattr(logging, file_log_level_str, logging.INFO)

# Setup mandatory console and file logging
setup_logging(
    log_folder=LOG_FOLDER,
    log_file_name="orchestrator.log",
    console_level=console_log_level,
    file_level=file_log_level,
)

logger = logging.getLogger(__name__)

# Module level connection string
DB_CONNECTION_STRING: str | None = None


def on_file_stable(
    data_path: str, companion_path: str | None, config: SourceConfig
) -> None:
    logger.info(
        "[PAIR READY] %s (source: %s, companion: %s)",
        data_path,
        config.name,
        companion_path or "none",
    )
    # Pass data_path and companion_path to loader for downstream processing and mapping
    # Files are already in data/input/
    # --- downstream processing here ---
    try:
        if MSSQL_ENABLED:
            # Instantiate a File Log Row
            filename = Path(data_path).name
            sourcename = config.name
            file_hash = generate_file_hash(data_path)
            # Extract date from filename using the source config pattern
            match = re.search(config.date_pattern, filename)
            if not match:
                raise ValueError(
                f"Cannot extract date from filename '{filename}' "
                f"using pattern '{config.date_pattern}'"
            )
            file_date = datetime.strptime(match.group(1), config.date_format)
            detected_at = datetime.now()
            conn = get_db_connection(DB_CONNECTION_STRING)
            cursor = conn.cursor()
            insert_query = f"""
                INSERT INTO Admin.FileLog
                (FileName, FileSource, FileHash, FileDate, Detected_at)
                VALUES (?,?,?,?,?)
                SELECT SCOPE_IDENTITY()
                ;
            """
            cursor.execute(
                insert_query,
                (
                    filename,
                    sourcename,
                    file_hash,
                    file_date,
                    detected_at
                ),
            )
            file_id = cursor.fetchone()[0]
            conn.commit()
            cursor.close()
            conn.close()
            # Check for duplicate file hash before inserting and processing - if duplicate, log and move to invalid folder
            conn = get_db_connection(DB_CONNECTION_STRING)
            cursor = conn.cursor()
            dup_query = """
                SELECT COUNT(*) FROM Admin.FileLog
                WHERE FileHash = ?
                ;
            """
            cursor.execute(dup_query, (file_hash,))
            dup_count = cursor.fetchone()[0]
            cursor.close() 
            conn.close()
            if dup_count > 1:
                # Mark the FileID entry as DUPLICATE in the database
                conn = get_db_connection(DB_CONNECTION_STRING)
                cursor = conn.cursor()
                dup_update_query = """
                    UPDATE Admin.FileLog
                    SET REsult = 'DUPLICATE'
                    WHERE FileID = ?
                """
                cursor.execute(dup_update_query, (file_id,))
                conn.commit()   
                cursor.close()
                conn.close()

                logger.warning(
                    "Duplicate file detected based on hash for file %s. Moving to invalid folder.",
                    data_path
                )
                dest = INVALID_FOLDER / Path(data_path).name
                atomic_move(Path(data_path), dest, generate_unique=True)
                return

        run_loader(config, data_path, companion_path, DB_CONNECTION_STRING, file_id)
        logger.info("Loader Complete: %s processed successfully", data_path)
        # Move file to processed folder
        dest = PROCESSED_FOLDER / Path(data_path).name
        atomic_move(Path(data_path), dest, generate_unique=True)
        
        logger.info("Moved %s to processed folder", data_path)
    except Exception:
        logger.exception("Loader Failed. Error processing %s", data_path)
        # Move file to invalid folder
        dest = INVALID_FOLDER / Path(data_path).name
        atomic_move(Path(data_path), dest, generate_unique=True)
        logger.info("Moved %s to quarantine due to processing failure", data_path)
        
watcher = FileWatcher(
    watch_dir=LANDING_FOLDER,
    callback=on_file_stable,
    input_dir=INPUT_FOLDER,
    quarantine_dir=INVALID_FOLDER,
    unknown_dir=UNKNOWN_FOLDER,
    recursive=False,
)


def main():
    global DB_CONNECTION_STRING

    if MSSQL_ENABLED:
        DB_CONNECTION_STRING = build_connection_string()
        if not DB_CONNECTION_STRING:
            logger.critical("MSSQL_ENABLED is true but the connection parameters are missing. Exiting...")
            sys.exit(1)
        try:
            conn = get_db_connection(DB_CONNECTION_STRING)
            conn.close()
            logger.info("Database connectivity verified.")
        except Exception as e:
            logger.critical("MSSQL_ENABLED is true but database is unreachable. Exiting...", exc_info=True)
            sys.exit(1)
        # SQL connection confirmed and verified - add SQL log handler
        add_sql_handler(DB_CONNECTION_STRING)

    if HEARTBEAT_ENABLED:
        heartbeat_file = Path("/tmp/orchestrator_heartbeat")
        create_heartbeat(heartbeat_file)
    
    watcher.start()


if __name__ == "__main__":
    main()
