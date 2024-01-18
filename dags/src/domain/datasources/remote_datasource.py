from abc import ABC, abstractmethod
from pandas import DataFrame
class RemoteDatasource(ABC):
    @abstractmethod
    def getDataPetroperu(self, url: str):
        ...
    @abstractmethod
    def getDataBCRP(self, url: str) -> DataFrame:
        ...
    @abstractmethod
    def getDataEIA1(self, url: str) -> DataFrame:
        ...
    @abstractmethod
    def getDataEIA2(self, url: str) -> DataFrame:
        ...
    @abstractmethod
    def joinBcrpAndEia(self, dfBcrp: DataFrame, dfEia1: DataFrame, dfEia2: DataFrame) -> DataFrame:
        ...
    @abstractmethod
    def getDataOsinergmin(self, url: str):
        ...
    @abstractmethod
    def getDataSigneBlock(self, url: str):
        ...