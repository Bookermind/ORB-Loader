import hashlib
import logging
import os
import tempfile
from datetime import datetime
from pathlib import Path
import pyodbc

logger = logging.getLogger(__name__)

def build_connection_string(
        db_driver: str = None,
        db_host: str = None,
        db_name: str = None,
        db_user: str = None,
        db_password: str = None,
        db_port: int = 1433,
) -> str | None:
    """
    Build a SQL connection string from parameters of environment variables.
    Args:
        db_driver: Database driver.
        db_host: Database host address.
        db_name: Database name.
        db_user: Database username.
        db_password: Database password.
        db_port: Database port number (default is 1433 for SQL Server). 
    Returns:
        A SQL connection string or None if required parameters are missing.
    """
    driver = db_driver or os.getenv("LOGGING_DB_DRIVER")
    host = db_host or os.getenv("LOGGING_DB_HOST")
    name = db_name or os.getenv("LOGGING_DB_NAME")
    port = db_port or int(os.getenv("LOGGING_DB_PORT", 1433))
    user = db_user or os.getenv("LOGGING_DB_USER")
    password = db_password or os.getenv("LOGGING_DB_PASSWORD")

    if not all([driver, host, name, user, password]):
        missing = [
            var for var, val in [
                ("LOGGING_DB_DRIVER", driver),
                ("LOGGING_DB_HOST", host),
                ("LOGGING_DB_NAME", name),
                ("LOGGING_DB_USER", user),
                ("LOGGING_DB_PASSWORD", password),
            ]
            if not val
        ]
        logger.warning("Missing database connection parameters: %s.", ", ".join(missing))
        return None
    
    return (
        f"DRIVER={{{driver}}};"
        f"SERVER={host},{port};"
        f"DATABASE={name};"
        f"UID={user};"
        f"PWD={password}"
    )

def get_db_connection(connection_string: str) -> pyodbc.Connection:
    """
    Establish and return a pyodbc connection.
    Args:
        connection_string: The SQL connection string.
    Return:
        A pyodbc Connection object.
    """
    return pyodbc.connect(connection_string, timeout=5)

def str_to_bool(value: str) -> bool:
    """
    Convert a string value to a boolean.
    """
    return value.lower() in ("true", "1", "yes", "on", "enabled")

def generate_unique_filename(source_path: Path, dest_path: Path) -> Path:
    """
    Generate a unique filename if a proposed file move would overwrite an existing file.
    Unique filename is generating by adding the timestamp to the file name.
    Args:
        source_path: The original file path.
        dest_path: The proposed destination file path.
    Returns:
        A unique file path that does not overwrite existing files.
    """
    if not dest_path.exists():
        return dest_path
    
    stem = source_path.stem
    suffix = source_path.suffix
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_filename = f"{stem}_{timestamp}{suffix}"
    unique_filepath = dest_path.parent / unique_filename

    return unique_filepath

def atomic_move(
        source_path: Path, dest_path: Path, generate_unique: bool = False
) -> Path:
    """
    Atomically move a file from source to destination. If generate_unique is True, it will generate a unique filename if the destination file already exists.
    Args:
        source_path: The original file path.
        dest_path: The proposed destination file path.
        generate_unique: Whether to generate a unique filename if the destination file already exists.
    Returns:
        The final destination path of the moved file.
    """
    if not source_path.exists():
        logger.error(
            "File has disappeared before a move could be attempted: %s", source_path
        )

    dest_folder = dest_path.parent
    dest_folder.mkdir(parents=True, exist_ok=True)

    if generate_unique:
        unique_dest_path = generate_unique_filename(source_path, dest_path)
    else:
        unique_dest_path = dest_path

    try:
        os.rename(source_path, unique_dest_path)
    except OSError:
        with tempfile.NamedTemporaryFile(dir=dest_folder, delete=False) as tmp:
            with open(source_path, "rb") as src:
                tmp.write(src.read())
            tmp_path = tmp.name

        os.rename(tmp_path, unique_dest_path)
        source_path.unlink()
    return unique_dest_path

def generate_file_hash(filepath: str) -> bytes:
    sha256 = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(8192):
            sha256.update(chunk)
    return sha256.digest()