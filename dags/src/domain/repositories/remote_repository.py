from abc import ABC, abstractmethod
from pandas import DataFrame
class RemoteRepository(ABC):
    @abstractmethod
    def getDataPetroperu(self, url: str):
        ...
    @abstractmethod
    def getDataMarcadores(self, urlBcrp: str, urlEia: str) -> DataFrame:
        ...
    @abstractmethod
    def getDataOsinergmin(self, url: str):
        ...
    @abstractmethod
    def getDataMinorista(self, url: str):
        ...