from abc import ABC, abstractmethod
from pandas import DataFrame
class DbDatasource(ABC):
    @abstractmethod
    def saveReferenciaOsinergmin(self, df: DataFrame):
        pass
    @abstractmethod
    def saveMayoristaPetroperu(self, petroperuDataframe: DataFrame):
        pass
    @abstractmethod
    def savePlanta(self, petroperuDataframe: DataFrame):
        pass
    @abstractmethod
    def saveMarcadores(self, marcadoresDataframe: DataFrame):
        pass
    @abstractmethod
    def saveRazonSocial(self):
        pass
    @abstractmethod
    def saveDirection(self):
        pass
    @abstractmethod
    def saveActivity(self):
        pass
    @abstractmethod
    def saveProduct(self):
        pass
    @abstractmethod
    def saveUbication(self):
        pass
    @abstractmethod
    def saveCodigoOsinerg(self):
        pass
    @abstractmethod
    def savePrice(self, data: DataFrame):
        pass
    @abstractmethod
    def saveRelapasa(self, df_combinado: DataFrame) -> DataFrame:
        pass