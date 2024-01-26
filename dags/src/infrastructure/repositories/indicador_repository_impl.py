
from src.domain.datasources.db_datasource import DbDatasource
from src.domain.datasources.file_datasource import FileDatasource
from src.domain.repositories.indicador_repository import IndicadorRepository


class IndicadorRepositoryImpl(IndicadorRepository):
    
    def __init__(self, fileDatasource: FileDatasource, dbDatasource: DbDatasource):
        self.fileDatasource = fileDatasource
        self.dbDatasource = dbDatasource
    
    def saveAndProcessData(self):
        self.fileDatasource.min4_a1_processMerge()
        self.dbDatasource.saveIndicadores()