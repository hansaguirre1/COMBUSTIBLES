from pandas import DataFrame
from src.domain.datasources.db_datasource import DbDatasource
from src.domain.datasources.file_datasource import FileDatasource
from src.domain.repositories.marcadores_repository import MarcadoresRepository


class MarcadoresRepositoryImpl(MarcadoresRepository):
    
    def __init__(self, fileDatasource: FileDatasource, dbDatasource: DbDatasource):
        self.fileDatasource = fileDatasource
        self.dbDatasource = dbDatasource
    
    
    def saveData(self, df: DataFrame):
        self.dbDatasource.saveMarcadores(marcadoresDataframe=df)
        self.fileDatasource.saveDataMarcadoresToCsv(df_combinado=df)