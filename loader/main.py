import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Optional

from loader.config.mappings_config import load_mapping
from loader.readers.factory import get_reader
from loader.transformers.schema_mapper import SchemaMapper
from loader.validators.record_validator import validate_data_file
from orchestrator.managers.source_identifier import SourceConfig
from orchestrator.utils.utilities import get_db_connection, str_to_bool

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_FOLDER = os.getenv("CONFIG_FOLDER_PATH")
CONFIG_DIR = Path(CONFIG_FOLDER) if CONFIG_FOLDER else PROJECT_ROOT / "config"
MAPPING_DIR = CONFIG_DIR / "mappings"
MSSQL_ENABLED = str_to_bool(os.getenv("MSSQL_ENABLED", "False"))

logger = logging.getLogger(__name__)


def validate_fields(mapping_config, actual_fields) -> bool:
    """
    Validates that the fields in the input file match the expected fields defined in the mapping config.
    Args:
        mapping_config (dict): The mapping configuration containing the expected fields.
        actual_fields (pd.Index or list): The set of fields present in the input file.
    """
    expected_fields = set()
    expected_positions = set()

    for target_field, config in mapping_config.items():
        if isinstance(config, str):
            # Simple mapping: target - source
            expected_fields.add(config)
        elif isinstance(config, int):
            expected_positions.add(config)
        else:
            # Complex mapping with source and transforms
            source = config.get("source")
            position = config.get("position")
            if source is not None:
                if isinstance(source, int):
                    expected_positions.add(source)
                else:
                    expected_fields.add(source)
            elif position is not None:
                expected_positions.add(position)

    # Convert actual_fields to a set for comparison
    actual_fields_set = set(actual_fields)

    # Validate named fields (string keys from headed files)
    if expected_fields:
        missing = expected_fields - actual_fields_set
        if missing:
            logger.critical("Expected fields not present in the data file: %s", missing)
            return False
        unexpected = actual_fields_set - expected_fields
        if unexpected:
            logger.warning(
                "There are unexpected fields present in the data file: %s", unexpected
            )

    # Validate positional fields (integer keys from headerless files)
    if expected_positions:
        max_position = max(expected_positions)
        actual_count = len(actual_fields_set)
        if max_position >= actual_count:
            logger.critical(
                "File mapping references %s fields, but only %s are present",
                max_position,
                actual_count,
            )
            return False
        if actual_count > max_position:
            logger.warning(
                "There are more fields present in the data file (%s) than expected based on mapping positions (%s)",
                actual_count,
                max_position,
            )
    return True


def run(
    source_config: SourceConfig,
    file_path: str,
    companion_path: Optional[str] = None,
    connection_string: Optional[str] = None,
    file_id: Optional[int] = None,
) -> None:
    """
    Main entry point for the loader.
    Args:
        source_config (SourceConfig): The configuration object for the data source.
        file_path (str): The path to the input file to be processed.
        companion_path (str, optional): The path to the companion file for file strategy. Defaults to None.
        connection_string (str, optional): The database connection string for logging. Defaults to None.
        file_id (Any, optional): The identifier for the file being processed, used for logging
    """
    # Extract date from filename using the source config pattern
    filename = Path(file_path).name
    match = re.search(source_config.date_pattern, filename)
    if not match:
        raise ValueError(
            f"Cannot extract date from filename '{filename}' "
            f"using pattern '{source_config.date_pattern}'"
        )
    file_date = datetime.strptime(match.group(1), source_config.date_format).date()
    # Resolve versioned mapping config
    mapping_config = load_mapping(source_config.name, file_date, MAPPING_DIR)
    # Build the reader
    reader_kwargs = {
        "encoding": source_config.encoding,
        "has_header": source_config.has_header,
    }
    if source_config.delimiter:
        reader_kwargs["delimiter"] = source_config.delimiter

    # If we are dealing with a fix-width file, add the column specs to the reader kwargs
    if source_config.file_type == "fixed-width":
        reader_kwargs["column_specs"] = source_config.column_specs

    reader = get_reader(
        file_type=source_config.file_type,
        **reader_kwargs,
    )

    # Read and validate the data file (pandas version)
    if reader is None:
        raise ValueError(
            f"No reader available for file type: {source_config.file_type}"
        )
    raw_data = reader.read(file_path)
    if source_config.footer_size > 0:
        raw_data = raw_data.iloc[: -source_config.footer_size]
    if not raw_data.empty:
        actual_fields = raw_data.columns
        valid_fields = validate_fields(mapping_config.field_mappings, actual_fields)

        if valid_fields:
            filename = Path(file_path).name
            logger.info("Field validation passed for file '%s'", filename)
            stage_id: Optional[int] = None

            if MSSQL_ENABLED:
                if connection_string is None:
                    raise ValueError("Connection string is required for MSSQL logging")
                # Insert SQL record for stage 0 start
                conn = get_db_connection(connection_string)
                cursor = conn.cursor()
                insert_stage_query = """
                    INSERT INTO Admin.StageLog (FileID, Stage)
                    OUTPUT inserted.StageID
                    VALUES (?, 0)
                    ;
                """
                cursor.execute(insert_stage_query, (file_id,))
                row = cursor.fetchone()
                if row is None:
                    raise RuntimeError("Failed to insert stage record")
                stage_id = int(row[0])

                # Now update FileLog with the start time of the current stage
                update_filelog_query = """
                    UPDATE Admin.FileLog
                    SET StageID = ?, Stage_start = ?, Result = ?
                    WHERE FileID = ?
                    ;
                """
                cursor.execute(
                    update_filelog_query, (stage_id, datetime.now(), "PENDING", file_id)
                )
                conn.commit()
                cursor.close()
                conn.close()

            # Perform record count and amount validation
            file_valid = validate_data_file(
                source_config,
                raw_data,
                file_path,
                companion_path,
                stage_id,
                connection_string,
                MSSQL_ENABLED,
            )
            if file_valid:
                logger.info(
                    "Record count and amount validation passed for file '%s'", filename
                )
                # Insert SQL record for stage 0 complete and passed
                if MSSQL_ENABLED:
                    if connection_string is None:
                        raise ValueError(
                            "Connection string is required for MSSQL logging"
                        )
                    conn = get_db_connection(connection_string)
                    cursor = conn.cursor()
                    update_filelog_query = """
                        UPDATE Admin.FileLog
                        SET Stage_end = ?, Result = ?
                        Where StageID = ?
                        ;
                    """
                    cursor.execute(
                        update_filelog_query, (datetime.now(), "PASSED", stage_id)
                    )
                    conn.commit()
                    cursor.close()
                    conn.close()
                # Transform the data
                mapper = SchemaMapper(mapping_config.field_mappings)
                result = mapper.transform(raw_data)

                # Output the data to stdout for debug purposes.
                print(result)
            else:
                # Insert SQL record for stage 0 complete and failed
                if MSSQL_ENABLED:
                    if connection_string is None:
                        raise ValueError(
                            "Connection string is required for MSSQL logging"
                        )
                    conn = get_db_connection(connection_string)
                    cursor = conn.cursor()
                    update_filelog_query = """
                        UPDATE Admin.FileLog
                        SET Stage_end = ?, Result = ?
                        Where StageID = ?
                        ;
                    """
                    cursor.execute(
                        update_filelog_query, (datetime.now(), "FAILED", stage_id)
                    )
                    conn.commit()
                    cursor.close()
                    conn.close()
                logger.critical(
                    "Record count and amount validation failed for file '%s'", filename
                )
                raise ValueError(
                    f"Record count and amount validation failed for file: {filename}"
                )
        else:
            raise ValueError(f"Field validation failed for file: {filename}")


if __name__ == "__main__":
    from orchestrator.managers.source_identifier import SourceConfig, load_sources

    sources = load_sources()
    config = sources["sapar"]
    run(config, "C:/Users/F27771/git/New Project/ORB-Loader/data/sapar/sapar_data.csv")
