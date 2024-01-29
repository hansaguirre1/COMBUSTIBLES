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
    
    @abstractmethod
    def ubi0_processUbigeo(self) -> DataFrame:
        pass
    
    @abstractmethod
    def m1_processCleanAndJoin(self):
        pass
    
    @abstractmethod
    def m2_processRucAndDays(self):
        pass
   
    @abstractmethod
    def dis3_processDistances(self):
        pass
    
    @abstractmethod
    def minfut4_processSeparacion(self):
        pass
    
    @abstractmethod
    def min4_a1_processMerge(self):
        pass
    
    @abstractmethod
    def exportFinalDta(self):
        pass
    
    @abstractmethod
    def divideIndicadoresFile(self):
        pass