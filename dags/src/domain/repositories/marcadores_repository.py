from abc import ABC, abstractmethod
from pandas import DataFrame
class MarcadoresRepository(ABC):
    @abstractmethod
    def saveData(self, df: DataFrame):
        pass
        