from .csv_reader import CSVReader
from .txt_reader import TXTReader
from .fwf_reader import FWFReader

import logging

logger = logging.getLogger(__name__)

def get_reader(file_type, **kwargs):
    """
    Factory function to return the appropriate reader based on file type
    """
    if file_type == 'csv':
        return CSVReader(**kwargs)
    elif file_type == 'txt':
        return TXTReader(**kwargs)
    elif file_type == 'fixed-width':
        return FWFReader(**kwargs)
    elif file_type == 'tab' or file_type == 'no_extension':
        # Tab delimited file assumed based on sap model (tab delimited txt with no file extension)
        kwargs.setdefault('delimiter', '\t')
        return TXTReader(**kwargs)
    else:
        logger.error("Unsupported file type: %s", file_type)
        return None