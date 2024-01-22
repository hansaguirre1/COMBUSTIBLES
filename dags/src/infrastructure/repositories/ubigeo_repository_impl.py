from src.domain.repositories.ubigeo_repository import UbigeoRepository
from src.domain.datasources.db_datasource import DbDatasource
from src.domain.datasources.file_datasource import FileDatasource


class UbigeoRepositoryImpl(UbigeoRepository):
    def __init__(self, fileDatasource: FileDatasource, dbDatasource: DbDatasource):
        self.fileDatasource = fileDatasource
        self.dbDatasource = dbDatasource
        
    def saveDataUbigeo(self):
        self.fileDatasource.ubi0_processUbigeo()
        self.dbDatasource.saveUbication()
        
    