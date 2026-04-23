from abc import ABC, abstractmethod
import pandas as pd

class FileReader(ABC):
    @abstractmethod
    def read(self, file_path) -> pd.DataFrame:
        """
        Abstract base method to read a file and return a pandas dataframe.
        """
        pass