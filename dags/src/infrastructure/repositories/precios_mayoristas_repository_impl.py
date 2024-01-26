from src.domain.repositories.precios_mayoristas_repository import PreciosMayoristasRepository
from src.domain.datasources.db_datasource import DbDatasource
from src.domain.datasources.file_datasource import FileDatasource


class PreciosMayoristasRepositoryImpl(PreciosMayoristasRepository):
    def __init__(self, fileDatasource: FileDatasource, dbDatasource: DbDatasource):
        self.fileDatasource = fileDatasource
        self.dbDatasource = dbDatasource
        
    def saveDataMayoristas(self):
        self.fileDatasource.m1_processCleanAndJoin()
        self.fileDatasource.m2_processRucAndDays()
    