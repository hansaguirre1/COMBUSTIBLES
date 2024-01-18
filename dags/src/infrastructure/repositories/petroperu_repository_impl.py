from src.domain.datasources.db_datasource import DbDatasource
from src.domain.datasources.file_datasource import FileDatasource
from src.domain.repositories.petroperu_repository import PetroperuRepository


class PetroperuRepositoryImpl(PetroperuRepository):
    def __init__(self, fileDatasource: FileDatasource, dbDatasource: DbDatasource):
        self.fileDatasource = fileDatasource
        self.dbDatasource = dbDatasource
        
    def saveData(self):
        df = self.fileDatasource.processFilesPetroperu()
        self.dbDatasource.savePlanta(df)
        self.dbDatasource.saveMayoristaPetroperu(df)
        self.fileDatasource.saveDataPetroperuToCSV(df)
        # self.fileDatasource.saveDataPetroperuToExcel(df)