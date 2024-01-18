from abc import ABC, abstractmethod
from pandas import DataFrame
class ReferenciaRepository(ABC):
    @abstractmethod
    def saveDataReferencia(self):
        pass