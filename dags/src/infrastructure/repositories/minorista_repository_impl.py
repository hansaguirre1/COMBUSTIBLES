from pandas import DataFrame
from src.domain.repositories.minorista_repository import MinoristaRepository
from src.domain.datasources.db_datasource import DbDatasource
from src.domain.datasources.file_datasource import FileDatasource


class MinoristaRepositoryImpl(MinoristaRepository):
    
    def __init__(self, fileDatasource: FileDatasource, dbDatasource: DbDatasource):
        self.fileDatasource = fileDatasource
        self.dbDatasource = dbDatasource
    
    
    def saveDataBase(self):
        data = self.fileDatasource.processFileMinoristasDiario()
        relapasa = self.dbDatasource.saveRelapasa(data)
        self.fileDatasource.saveDataRelapasaToCsv(relapasa)
        
        # self.dbDatasource.saveRazonSocial()
        # self.dbDatasource.saveCodigoOsinerg()
        # self.dbDatasource.saveUbication()
        # self.dbDatasource.saveActivity()
        # self.dbDatasource.saveProduct()
        # self.dbDatasource.saveDirection()
        # self.dbDatasource.savePrice(data=data)
    
    def saveDataToDta(self):
        self.fileDatasource.exportFinalDta()
        