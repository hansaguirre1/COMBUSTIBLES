from abc import ABC, abstractmethod
from pandas import DataFrame

class FileDatasource(ABC):
    
    @abstractmethod
    def processFileOsinergminReferencia(self) -> DataFrame:
        pass
    
    @abstractmethod
    def processFilesPetroperu(self) -> DataFrame:
        pass
    @abstractmethod
    def saveDataPetroperuToCSV(self, df_combinado: DataFrame):
        pass
    @abstractmethod
    def saveDataPetroperuToExcel(self, df_combinado: DataFrame):
        pass
    @abstractmethod
    def saveDataMarcadoresToCsv(self, df_combinado: DataFrame):
        pass
    @abstractmethod
    def saveDataRelapasaToCsv(self, df_combinado: DataFrame):
        pass
    @abstractmethod
    def processFileMinoristasDiario(self) -> DataFrame:
        pass
    
    @abstractmethod
    def cv1_processReadAndCleanNewValues(self) -> DataFrame:
        pass
    
    @abstractmethod
    def cv2_processCombustiblesValidos(self) -> DataFrame:
        pass

    # @abstractmethod
    # def saveDataActivityCsv(self, data: DataFrame) -> DataFrame:
    #     pass
    # @abstractmethod
    # def saveDataDirectionCsv(self, data: DataFrame) -> DataFrame:
    #     pass
    # @abstractmethod
    # def saveDataProductCsv(self, data: DataFrame) -> DataFrame:
    #     pass
    # @abstractmethod
    # def saveDataCodigoOsinergCsv(self, data: DataFrame, df_actividad: DataFrame) -> DataFrame:
    #     pass
    # @abstractmethod
    # def saveDataRazonSocialCsv(self, data: DataFrame, df_ubigeo: DataFrame) -> DataFrame:
    #     pass
    # @abstractmethod
    # def saveDataUbicationCsv(self, data: DataFrame) -> DataFrame:
    #     pass
    @abstractmethod
    def exportFinalDta(self):
        pass