from datetime import datetime
from typing import Any, Dict, Optional

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
    parts = value.split(delim)
    return parts[index] if index < len(parts) else ''

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
    dt = datetime.strptime(value, input_format)
    return dt.strftime(output_format)

def trans_substring(value: str, start: int, length: Optional[int] = None, end: Optional[int] = None) -> str:
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
    if length is not None:
        return value[start:start + length] if start < len(value) else ''
    elif end is not None:
        return value[start:end] if start < len(value) else ''
    else:
        return value[start:] if start < len(value) else ''

def apply_single_transform(value, transform_type, args, row=None, named_results=None):
    """
    Applies a single transformation to a value. The following transformations ae supported:
    1. Split: Splits a string based on a delimiter and returns a specificed part of the string.
    2. Date Format: Converts a string into a date. String is accepted with a defined date format.
       The returned string is formatted according to the output format.
    3. Substring: Extracts a substring from an incoming string. Two methods are supported:
         - Using a start index and length.
         - Using a start index and an end index.
    4. Lookup: Maps an input value to an output value based on a lookup table/dictionary.
    5. Concat: Concatenates multiple values together.
        Supports $value (current value), $fieldname (row_fields), $name (named_results)
    Args:
        value: The value to be transformed.
        transform_type: The type of transformation to apply (e.g., 'split', 'date_format', 'substring', 'lookup', 'concat').
        args: The arguments required for the specified transformation type.
        row: Optional. The current row being processed, used for field references in transformations.
        named_results: Optional. A dictionary of named results from previous transformations, used for references in transformations.
    Returns:
        The transformed value based on the specified transformation type and arguments.
    """
    if value is None:
        return None
    
    if transform_type.lower() == 'split':
        return trans_split(value, args.get('delimiter',','), args.get('index',0))
    elif transform_type.lower() == 'date_format':
        return trans_date_format(value, args.get('input_format'), args.get('output_format'))
    elif transform_type.lower() == 'substring':
        return trans_substring(value, args.get('start',0), args.get('length'), args.get('end'))
    elif transform_type.lower() == 'lookup':
        # Map input values to ouput values
        mapping = args.get('mapping', {})
        default = args.get('default')
        # Get the mapped value
        mapped_value = mapping.get(value, default if default is not None else value)
        #Check if the mapped value is a field reference (starts with a '$')
        if isinstance(mapped_value, str) and mapped_value.startswith('$') and row:
            field_name = mapped_value[1:] # Remove the '$' prefix
            return row.get(field_name, mapped_value)
        return mapped_value
    elif transform_type.lower() == 'concat':
        # Concatenate multiple fields are values together
        values_to_concat = args.get('values', [])
        sperator = args.get('seperator','')
        result_parts = []

        for item in values_to_concat:
            if isinstance(item, str) and item.startswith('$'):
                if item.lower() == '$value':
                    # Special references - use the incoming transformed value
                    result_parts.append(str(value) if value is not None else '')
                elif named_results and item[1:] in named_results:
                    # Named result refernce - get value from named results
                    named_value = named_results.get(item[1:],'')
                    result_parts.append(str(named_value))
                elif row:
                    # Field reference - get value from row
                    field_name = item[1:]
                    field_value = row.get(field_name,'')
                    result_parts.append(str(field_value))
                else:
                    # No context - use string literal
                    result_parts.append(str(item))
            else:
                # Static value - use as-is
                result_parts.append(str(item))
        return sperator.join(result_parts)
    return value