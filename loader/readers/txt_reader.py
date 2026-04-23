from .base import FileReader
import pandas as pd

class TXTReader(FileReader):
    def __init__(self, delimiter='\t', encoding='utf-8-sig', has_header=True):
        self.delimiter = delimiter
        self.encoding = encoding
        self.has_header = has_header

    def read(self, file_path) -> pd.DataFrame:
        return pd.read_csv(
            file_path,
            delimiter=self.delimiter,
            encoding=self.encoding,
            header=0 if self.has_header else None
        )