from src.infrastructure.repositories.precios_mayoristas_repository_impl import PreciosMayoristasRepositoryImpl
from src.infrastructure.repositories.ubigeo_repository_impl import UbigeoRepositoryImpl
from src.infrastructure.repositories.combustibles_validos_repository_impl import CombustiblesValidosRepositoryImpl
from src.infrastructure.repositories.referencia_repository_impl import ReferenciaRepositoryImpl
from src.infrastructure.repositories.minorista_repository_impl import MinoristaRepositoryImpl
from src.infrastructure.repositories.marcadores_repository_impl import MarcadoresRepositoryImpl
from src.infrastructure.repositories.remote_repository_impl import RemoteRepositoryImpl
from src.infrastructure.datasources.remote_datasource_impl import RemoteDatasourceImpl
from src.infrastructure.datasources.db_datasource_impl import DbDatasourceImpl
from src.infrastructure.datasources.file_datasource_impl import FileDatasourceImpl
from src.infrastructure.repositories.petroperu_repository_impl import MayoristaPetroperuRepositoryImpl
from src.config.db_config import Database
from dependency_injector import containers, providers

class Container(containers.DeclarativeContainer):
    
    db = providers.Singleton(Database)
     
    file_datasource = providers.Factory(
        FileDatasourceImpl
    )
    
    remote_datasource = providers.Factory(
        RemoteDatasourceImpl
    )
    
    db_datasource = providers.Factory(
        DbDatasourceImpl,
        session_factory=db.provided.session,
    )
     
    petroperu_repository = providers.Factory(
        MayoristaPetroperuRepositoryImpl,
        fileDatasource=file_datasource,
        dbDatasource=db_datasource,
    )
    
    referencia_repository = providers.Factory(
        ReferenciaRepositoryImpl,
        fileDatasource=file_datasource,
        dbDatasource=db_datasource,
    )
    
    remote_repository = providers.Factory(
        RemoteRepositoryImpl,
        remoteDatasource=remote_datasource
    )
    
    marcador_repository = providers.Factory(
        MarcadoresRepositoryImpl,
        fileDatasource=file_datasource,
        dbDatasource=db_datasource
    )
    
    minorista_repository = providers.Factory(
        MinoristaRepositoryImpl,
        fileDatasource=file_datasource,
        dbDatasource=db_datasource
    )
    
    combustible_valido_repository = providers.Factory(
        CombustiblesValidosRepositoryImpl,
        fileDatasource=file_datasource,
        dbDatasource=db_datasource
    )
    
    ubigeo_repository = providers.Factory(
        UbigeoRepositoryImpl,
        fileDatasource=file_datasource,
        dbDatasource=db_datasource
    )
    
    precios_mayoristas_repository = providers.Factory(
        PreciosMayoristasRepositoryImpl,
        fileDatasource=file_datasource,
        dbDatasource=db_datasource
    )