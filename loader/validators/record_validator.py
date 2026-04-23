import re
import logging
import pandas as pd
from decimal import Decimal
from pathlib import Path
from orchestrator.utils.utilities import get_db_connection

logger = logging.getLogger(__name__)

def extract_footer_text(file_path: str, footer_size: int, encoding: str) -> str:
    """
    Reads the last N lines of a file as defined by the footer size
    Args:
        file_path (str): The path to the file to read.
        footer_size (int): The number of lines to read from the end of the file.
        encoding (str): The encoding of the file.
    Returns:
        str: The extracted footer text.
    """
    with open(file_path, encoding=encoding) as f:
        lines = f.readlines()
    footer_lines = lines[-footer_size:]
    return ''.join(footer_lines)

def extract_expected_values(text: str, count_pattern: str, amount_pattern: str):
    """
    Extract the expected count and amount from text using regex patterns.
    Args:
        text (str): The text to search for the expected values.
        count_pattern (str): The regex pattern to extract the expected count.
        amount_pattern (str): The regex pattern to extract the expected amount.
    Returns:
        expected_count (int): The extracted expected count.
        expected_amount (Decimal): The extracted expected amount.
    """
    count_match = re.search(count_pattern, text)
    if not count_match:
        raise ValueError(f"Count Pattern '{count_pattern}' not found in {text}.")
    expected_count = int(count_match.group(1))

    amount_match = re.search(amount_pattern, text)
    if not amount_match:
        raise ValueError(f"Amount Pattern '{amount_pattern}' not found in {text}.")
    
    raw_amount = amount_match.group(1)
    # Strip currency symbols and commas, then convert to Decimal
    cleaned_amount = re.sub(r'[£$€,]', '', raw_amount)
    expected_amount = Decimal(cleaned_amount)

    return expected_count, expected_amount

def calculate_actual_values(df: pd.DataFrame, source_config) -> tuple:
    """
    Calculate the actual count and amount from data rows.
    Args:
        df (pd.DataFrame): The DataFrame containing the data rows to calculate values from.
        source_config: The configuration object containing field mappings.
    Returns:
        actual_count (int): The calculated actual count.
        actual_amount (Decimal): The calculated actual amount.
    """
    actual_count = len(df)

    if source_config.amount_column_name:
        amount_key = source_config.amount_column_name.lower()
    elif source_config.amount_column_position is not None:
        amount_key = source_config.amount_column_position -1 # 0-indexed
    else:
        raise ValueError("Amount column not defined in source configuration.")
    
    # Clean and sum the amount column
    cleaned_amounts = df[amount_key].astype(str).str.replace(r'[£$€,]', '', regex=True)
    actual_amount = cleaned_amounts.apply(Decimal).sum()
    
    return actual_count, actual_amount

def validate_data_file(
        source_config,
        df: pd.DataFrame,
        file_path: str,
        companion_path: str = None,
        stage_id: int = None,
        connection_string: str = None,
        db_enabled: bool = False
) -> bool:
    """
    Validate the record count and amount totals.
    For footer strategy - extracts expected values from the data file footer
    For file strategy - extracts expected values from a companion file
    Args:
        source_config: The configuration object containing validation settings.
        df (pd.DataFrame): The DataFrame containing the data rows to validate.
        file_path (str): The path to the data file being validated.
        companion_path (str, optional): The path to the companion file for file strategy. Defaults to None.
        stage_id (int, optional): The ID of the current stage in the process. Defaults to None.
        connection_string (str, optional): The connection string for the database. Defaults to None.
        db_enabled (bool, optional): Flag indicating if database operations are enabled. Defaults to False.
    Returns:
        bool: True if validation passes, False otherwise.
    """
    # Get expected values
    if source_config.validation_strategy == 'footer':
        footer_text = extract_footer_text(
            file_path, source_config.footer_size, source_config.encoding
        )
        expected_count, expected_amount = extract_expected_values(
            footer_text, source_config.count_pattern, source_config.amount_pattern
        )
    elif source_config.validation_strategy == 'file':
        if not companion_path:
            raise ValueError("File validation strategy required a companion file but none was provided.")
        companion_text = Path(companion_path).read_text(encoding=source_config.encoding)
        expected_count, expected_amount = extract_expected_values(
            companion_text, source_config.count_pattern, source_config.amount_pattern
        )
    else:
        raise ValueError(f"Unknown validation strategy: {source_config.validation_strategy}")
    
    # Get the actual values
    actual_count, actual_amount = calculate_actual_values(df, source_config)

    # Compare and validate data file
    # SQL Log into StageLog with results of the validation
    # NOTE: The result and closing of the stage (into FileLog) are handled in the calling function
    if db_enabled:
        conn = get_db_connection(connection_string)
        cursor = conn.cursor()
        update_query = """
            UPDATE Admin.StageLog
            SET Expected_count = ?, Expected_amount = ?, Actual_count = ?, Actual_amount = ?
            WHERE StageID = ?
        """
        cursor.execute(
            update_query,
            (
                expected_count,
                expected_amount,
                actual_count,
                actual_amount,
                stage_id
            )
        )
        conn.commit()
        cursor.close()
        conn.close()
    valid = True
    if actual_count != expected_count:
        logger.critical(
            "Record count mismatch for file %s: expected %d, got %d",
            file_path, expected_count, actual_count
        )
        valid = False
    if actual_amount != expected_amount:
        logger.critical(
            "Amount total mismatch for file %s: expected %s, got %s",
            file_path, expected_amount, actual_amount
        )
        valid = False

    if valid:
        logger.info(
            "Record validation passed for file %s: expected count %d, actual count %d; expected amount %s, actual amount %s",
            file_path, expected_count, actual_count, expected_amount, actual_amount
        )
    return valid