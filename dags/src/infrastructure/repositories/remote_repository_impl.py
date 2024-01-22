
from src.domain.repositories.remote_repository import RemoteRepository
from src.domain.datasources.remote_datasource import RemoteDatasource
from pandas import DataFrame

class RemoteRepositoryImpl(RemoteRepository):
    
    def __init__(self, remoteDatasource: RemoteDatasource):
        self.remoteDatasource = remoteDatasource
        
    
    def getDataPetroperu(self, url: str):
        self.remoteDatasource.getDataPetroperu(url=url)
    
    def getDataMarcadores(self, urlBcrp: str, urlEia: str) -> DataFrame:
        dfBcrp = self.remoteDatasource.getDataBCRP(url=urlBcrp)
        dfEia1 = self.remoteDatasource.getDataEIA1(url=urlEia)
        dfEia2 = self.remoteDatasource.getDataEIA2(url=urlEia)
        dfMarcadores = self.remoteDatasource.joinBcrpAndEia(dfBcrp, dfEia1, dfEia2)
        print('-----Marcador-----')
        print(dfMarcadores)
        print('-----Fin-Marcador-----')
        return dfMarcadores

    def getDataOsinergmin(self, url: str):
        self.remoteDatasource.getDataOsinergmin(url=url)
        
    def getDataMinorista(self, url: str):
        self.remoteDatasource.min0_A2_descarga(url=url)
        
    def cv0_getDataCombustiblesValidos(self, url: str):
        self.remoteDatasource.getDataCombustiblesValidos(url=url)
    
    def m0_descarga_mayorista(self):
        self.remoteDatasource.m0_descarga_mayorista()
        