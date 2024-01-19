from src.domain.repositories.combustibles_validos_repository import CombustiblesValidosRepository
from src.domain.datasources.db_datasource import DbDatasource
from src.domain.datasources.file_datasource import FileDatasource


class CombustiblesValidosRepositoryImpl(CombustiblesValidosRepository):
    
    def __init__(self, fileDatasource: FileDatasource, dbDatasource: DbDatasource):
        self.fileDatasource = fileDatasource
        self.dbDatasource = dbDatasource
        
    def processDataCombustiblesValidos(self):
        self.fileDatasource.cv1_processReadAndCleanNewValues()
        self.fileDatasource.cv2_processCombustiblesValidos()
        self.dbDatasource.saveCombustibleValido()