from datetime import datetime
from typing import Any, Dict, List, Optional, Union, cast

import pandas as pd

# from pandas.core.dtypes.dtypes import is_bool

# ===============================
# Scaler functions (kept for backwards compatability)
# ===============================


def trans_split(value: str, delim: str, index: int) -> str:
    """
    Splits a string based on a delimiter and returns a specificed part of the string.
    Args:
        value (str): The string to be split.
        delim (str): The delimiter to split the string.
        index (int): The index of the part to return after splitting.
    Returns:
        str: The specified part of the string after splitting.
    """
    if value is None:
        return ""
    parts = value.split(delim)
    return parts[index] if index < len(parts) else ""


def trans_date_format(value: str, input_format: str, output_format: str) -> str:
    """
    Converts a string into a date. String is accepted with a defined date format.
    The returned string is formatted according to the output format.
    Args:
        value (str): The date string to be converted.
        input_format (str): The format of the input date string.
        output_format (str): The desired format for the output date string.
    Returns:
        str: The converted date string in the specified output format.
    """
    if value is None:
        return ""
    dt = datetime.strptime(value, input_format)
    return dt.strftime(output_format)


def trans_substring(
    value: str, start: int, length: Optional[int] = None, end: Optional[int] = None
) -> str:
    """
    Extracts a substring from an incoming string. Two methods are supported:
    1. Using a start index and length.
    2. Using a start index and an end index.
    Args:
        value (str): The string from which to extract the substring.
        start (int): The starting index for the substring.
        length (Optional[int]): The length of the substring to extract. If not provided, end index will be used.
        end (Optional[int]): The ending index for the substring. If not provided, length will be used.
    Returns:
        str: The extracted substring based on the provided parameters.
    """
    if value is None:
        return ""
    if length is not None:
        return value[start : start + length] if start < len(value) else ""
    elif end is not None:
        return value[start:end] if start < len(value) else ""
    else:
        return value[start:] if start < len(value) else ""


# ===============================
# Vectorised transformations (for use with pandas series/dataframes)
# ===============================


def trans_split_vec(series: pd.Series, delim: str, index: int) -> pd.Series:
    """
    Vectorised split function - splits a string in a pandas series and returns a specified index of that string.
    Args:
        series (pd.Series): The pandas series containing the strings to be split.
        delim (str): The delimiter to split the strings.
        index (int): The index of the part to return after splitting.
    Returns:
        pd.Series: A new pandas series containing the specified part of the string after splitting.
    """
    result = series.astype(str).str.split(delim).str[index].fillna("")
    return pd.Series(result.values, index=series.index, dtype=str)


def trans_date_format_vec(
    series: pd.Series, input_format: str, output_format: str
) -> pd.Series:
    """
    Vectorised date format function - converts all dates in series based on an incoming and outgoing format.
    Args:
        series (pd.Series): The pandas series containing the date strings to be converted.
        input_format (str): The format of the input date strings.
        output_format (str): The desired format for the output date strings.
    Returns:
        pd.Series: A new pandas series containing the converted date strings in the specified output format.
    """

    def _format_item(x: Any) -> str:
        if pd.isna(x):
            return ""
        try:
            dt = datetime.strptime(str(x), input_format)
            return dt.strftime(output_format)
        except (ValueError, TypeError):
            return ""

    result = series.apply(_format_item)
    return pd.Series(result.values, index=series.index, dtype=str)


def trans_substring_vec(
    series: pd.Series,
    start: int,
    length: Optional[int] = None,
    end: Optional[int] = None,
) -> pd.Series:
    """
    Vectorised substring function - extracts a substring from a test field in a pandas string; based on the start and either the length of end index.
    Args:
        series (pd.Series): The pandas series containing the strings from which to extract substrings.
        start (int): The starting index for the substring.
        length (Optional[int]): The length of the substring to extract.
        end (Optional[int]): The ending index for the substring.
    Returns:
        pd.Series: A new pandas series containing the extracted substrings based on the provided parameters.
    """
    if length is not None:
        result = series.str[start : start + length]
    elif end is not None:
        result = series.str[start:end]
    else:
        result = series.str[start:]
    return pd.Series(result.fillna("").values, index=series.index, dtype=str)


def trans_lookup_vec(
    series: pd.Series, mapping: Dict, default: Any = None
) -> pd.Series:
    """
    Vectorised lookup transformation - maps input values in a pandas series to output values based on a lookup table/dictionary.
    Args:
        series (pd.Series): The pandas series containing the values to be mapped.
        mapping (Dict): A dictionary representing the lookup table, where keys are input values and values are the corresponding output values.
        default (Any, optional): A default value to return for input values that are not found in the mapping. If not provided, the original value will be returned.
    Returns:
        pd.Series: A new pandas series containing the mapped values based on the provided lookup table and default value.
    """

    def _map_item(x: Any) -> str:
        if pd.isna(x):
            return default if default is not None else ""
        return mapping.get(x, default if default is not None else x)

    result = series.apply(_map_item)
    return pd.Series(result.values, index=series.index, dtype=str)


def trans_concat_vec(
    values: List[Union[str, pd.Series]],
    separator: str,
    current_series: pd.Series,
    df: pd.DataFrame,
    named_results: Dict[str, pd.Series],
) -> pd.Series:
    """
    Vectorised concat function - concatenated multiple values or Series into one.
    Args:
        values (List[Union[str, pd.Series]]): A list of values or Series to concatenate.
        separator (str): The separator to use when concatenating the values.
        current_series (pd.Series): The current series being processed.
        df (pd.DataFrame): The DataFrame containing the data.
        named_results (Dict[str, pd.Series]): A dictionary of named results from previous transformations.
    Returns:
        pd.Series: A new pandas series containing the concatenated values.
    """
    parts_list: List[pd.Series] = []
    n = len(df)
    idx = df.index

    for item in values:
        if isinstance(item, pd.Series):
            parts_list.append(pd.Series(item.values, index=idx, dtype=str))
        elif isinstance(item, str):
            if item.lower() == "$value":
                parts_list.append(
                    pd.Series(current_series.values, index=idx, dtype=str)
                )
            elif item.startswith("$"):
                ref_name = item[1:]
                if ref_name in named_results:
                    parts_list.append(
                        pd.Series(named_results[ref_name].values, index=idx, dtype=str)
                    )
                elif ref_name in df.columns:
                    parts_list.append(
                        pd.Series(df[ref_name].values, index=df.index, dtype=str)
                    )
                else:
                    parts_list.append(pd.Series([item] * n, index=df.index, dtype=str))
            else:
                parts_list.append(pd.Series([str(item)] * n, index=df.index, dtype=str))
    result = pd.Series([""] * n, index=df.index, dtype=str)
    for parts in parts_list:
        result = result.str.cat(parts, sep=separator)
    return result


# ================================
# Main transform dispatcher function
# ===============================


def apply_single_transform(
    value: Union[Any, pd.Series],
    transform_type: str,
    args: Dict,
    row: Optional[Dict] = None,
    df: Optional[pd.DataFrame] = None,
    named_results: Optional[Dict[str, pd.Series]] = None,
):
    """
    Applies a single transformation to a value or pandas series based on the specified transform type and arguments.
    Args:
        value (Union[Any, pd.Series]): The input value or pandas series to be transformed.
        transform_type (str): The type of transformation to apply (e.g., 'split', 'date_format', 'lookup', 'concat').
        args (Dict): A dictionary of arguments required for the specified transformation type.
        row (Optional[Dict]): An optional dictionary representing the current row being processed (used for certain transformations).
        df (Optional[pd.DataFrame]): An optional DataFrame containing the data (used for certain transformations).
        named_results (Optional[Dict[str, pd.Series]]): An optional dictionary of named results from previous transformations (used for certain transformations).
    Returns:
        Union[Any, pd.Series]: The transformed value or pandas series after applying the specified transformation.
    """
    is_vectorised = isinstance(value, pd.Series)

    if not is_vectorised and value is None:
        return None

    transform_type = transform_type.lower()
    if transform_type == "split":
        delim = args.get("delimiter", ",")
        index = int(args.get("index", 0))
        if is_vectorised:
            return trans_split_vec(cast(pd.Series, value), delim, index)
        return trans_split(value, delim, index)
    elif transform_type == "date_format":
        input_format = str(args.get("input_format", "%Y%m%d"))
        output_format = str(args.get("output_format", "%d/%m/%Y"))
        if is_vectorised:
            return trans_date_format_vec(
                cast(pd.Series, value), input_format, output_format
            )
        return trans_date_format(cast(str, value), input_format, output_format)
    elif transform_type == "substring":
        start = int(args.get("start", 0))
        length = args.get("length")
        end = args.get("end")
        if is_vectorised:
            return trans_substring_vec(cast(pd.Series, value), start, length, end)
        return trans_substring(cast(str, value), start, length, end)
    elif transform_type == "lookup":
        mapping = dict(args.get("mapping", {}))
        default = args.get("default")
        if is_vectorised:
            return trans_lookup_vec(cast(pd.Series, value), mapping, default)
        mapped = mapping.get(value, default if default is not None else value)
        if isinstance(mapped, str) and mapped.startswith("$") and row:
            return row.get(mapped[1:], mapped)
        return mapped

    elif transform_type == "concat":
        values_to_concat = list(args.get("values", []))
        separator = str(args.get("separator", ""))
        if is_vectorised:
            return trans_concat_vec(
                values_to_concat,
                separator,
                cast(pd.Series, value),
                cast(pd.DataFrame, df),
                named_results or {},
            )

        result_parts = []
        for item in values_to_concat:
            if isinstance(item, str) and item.startswith("$"):
                if item.lower() == "$value":
                    result_parts.append(str(value) if value is not None else "")
                elif named_results and item[1:] in named_results:
                    result_parts.append(str(named_results[item[1:]]))
                elif row and item[1:] in row:
                    result_parts.append(str(row.get(item[1:], "")))
                else:
                    result_parts.append(str(item))
            else:
                result_parts.append(str(item))
        return separator.join(result_parts)

    return value
