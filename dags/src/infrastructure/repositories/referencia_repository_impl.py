from src.domain.repositories.referencia_repository import ReferenciaRepository
from src.domain.datasources.db_datasource import DbDatasource
from src.domain.datasources.file_datasource import FileDatasource
from src.domain.repositories.petroperu_repository import MayoristaPetroperuRepository


class ReferenciaRepositoryImpl(ReferenciaRepository):
    def __init__(self, fileDatasource: FileDatasource, dbDatasource: DbDatasource):
        self.fileDatasource = fileDatasource
        self.dbDatasource = dbDatasource
        
    def saveDataReferencia(self):
        df = self.fileDatasource.processFileOsinergminReferencia()
        self.dbDatasource.saveReferenciaOsinergmin(df)
        
    