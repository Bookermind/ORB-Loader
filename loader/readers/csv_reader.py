from .base import FileReader
import pandas as pd

class CSVReader(FileReader):
    def __init__(self, encoding='utf-8-sig', delimiter=',', has_header=True):
        self.encoding = encoding
        self.delimiter = delimiter
        self.has_header = has_header

    def read(self, file_path) -> pd.DataFrame:
        return pd.read_csv(
            file_path,
            delimiter=self.delimiter,
            encoding=self.encoding,
            header=0 if self.has_header else None
        )