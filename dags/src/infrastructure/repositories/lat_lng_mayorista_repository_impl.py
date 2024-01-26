from pandas import DataFrame
from src.domain.repositories.lat_lng_mayorista_repository import LatLngMayoristaRepository
from src.domain.datasources.db_datasource import DbDatasource
from src.domain.datasources.file_datasource import FileDatasource


class LatLngMayoristaRepositoryImpl(LatLngMayoristaRepository):
    
    def __init__(self, fileDatasource: FileDatasource, dbDatasource: DbDatasource):
        self.fileDatasource = fileDatasource
        self.dbDatasource = dbDatasource
    
    def saveDataLatLng(self):
        self.fileDatasource.dis3_processDistances()
        self.dbDatasource.saveMayMinGeo()
        