from .base import FileReader
import pandas as pd

class FWFReader(FileReader):
    def __init__(self, encoding='utf-8-sig', has_header=True, column_specs=None, names=None):
        self.encoding = encoding
        self.has_header = has_header
        self.colspecs = column_specs
        self.names = names
    
    def read(self, file_path) -> pd.DataFrame:
        return pd.read_fwf(
            file_path,
            encoding=self.encoding,
            colspecs=self.colspecs,
            names=None if self.has_header else self.names,
            header=0 if self.has_header else None
        )

#Example call for this function:
#reader = FWFReader(
#    encoding='utf-8',
#    column_specs=[(0, 10), (10, 20), (20, 35)],
#    names=['col1', 'col2', 'col3'],
#    has_header=False
#)
#df = reader.read('my_fixed_width_file.txt')