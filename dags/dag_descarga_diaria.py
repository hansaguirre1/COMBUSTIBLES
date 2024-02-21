from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta



with DAG(
        'Descarga-data-diaria',
        # These args will get passed on to each operator
        # You can override them on a per-task basis during operator initialization
        default_args={
            'depends_on_past': False,
            'email': ['hansaguirre10@gmail.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(seconds=1),
           
        },
        description='Digemid',
        schedule_interval="0 18 * * *",
        start_date=datetime(2021, 1, 1, 10, 15),
        catchup=False,
) as dag:
    
    def processDataMinoristas():
        from src.injection.containers import Container
        from src.domain.repositories.remote_repository import RemoteRepository
        from src.domain.repositories.minorista_repository import MinoristaRepository
        
        from src.config.get_env import url_signeblock

        container = Container()
        remoteRepository: RemoteRepository = container.remote_repository()
        # minoristaRepository: MinoristaRepository = container.minorista_repository()
        
        remoteRepository.getDataMinorista(url=url_signeblock)
        # minoristaRepository.saveDataBase()

    def processDataCombustiblesValidos():
        from src.injection.containers import Container
        from src.domain.repositories.remote_repository import RemoteRepository
        from src.domain.repositories.petroperu_repository import MayoristaPetroperuRepository
        from src.domain.repositories.combustibles_validos_repository import CombustiblesValidosRepository
        from src.config.get_env import url_petroperu

        container = Container()
        remoteRepository: RemoteRepository = container.remote_repository()
        combustibleValidoRepository: CombustiblesValidosRepository = container.combustible_valido_repository()
        
        remoteRepository.cv0_getDataCombustiblesValidos(url = '')
        combustibleValidoRepository.processDataCombustiblesValidos()

    def processDataPreciosMayoristaPetroperu():
        from src.injection.containers import Container
        from src.domain.repositories.remote_repository import RemoteRepository
        from src.domain.repositories.petroperu_repository import MayoristaPetroperuRepository
        from src.config.get_env import url_petroperu

        container = Container()
        remoteRepository: RemoteRepository = container.remote_repository()
        mayoristaPetroperuRepository: MayoristaPetroperuRepository = container.petroperu_repository()
        
        remoteRepository.getDataPetroperu(url=url_petroperu)
        mayoristaPetroperuRepository.saveData()

    def processDataPreciosReferencialesOsinergmin():
        from src.domain.repositories.remote_repository import RemoteRepository
        from src.domain.repositories.referencia_repository import ReferenciaRepository

        from src.config.get_env import url_osinergmin
        from src.injection.containers import Container
        
        container = Container()
        remoteRepository: RemoteRepository = container.remote_repository()
        referenciaRepository: ReferenciaRepository = container.referencia_repository()
        remoteRepository.getDataOsinergmin(url=url_osinergmin)
        referenciaRepository.saveDataReferencia()
    
    def processDataMarcadores():
        from src.config.get_env import url_bcrp, url_eia
        from src.injection.containers import Container
        from src.domain.repositories.remote_repository import RemoteRepository
        from src.domain.repositories.marcadores_repository import MarcadoresRepository

        container = Container()
        remoteRepository: RemoteRepository = container.remote_repository()
        marcadorRepository: MarcadoresRepository = container.marcador_repository()
        
        dfMarcadores = remoteRepository.getDataMarcadores(urlBcrp=url_bcrp, urlEia=url_eia)
        marcadorRepository.saveData(df=dfMarcadores)
    
    process_data_minoristas = PythonOperator(
        task_id='process-data-minoristas',
        python_callable=processDataMinoristas,
        dag=dag,
        )
    
    process_data_combustibles_validos = PythonOperator(
    task_id='process_data_combustibles_validos',
    python_callable=processDataCombustiblesValidos,
    dag=dag,
    )

    process_data_precios_mayorista_petroperu = PythonOperator(
        task_id='process_data_precios_mayorista_petroperu',
        python_callable=processDataPreciosMayoristaPetroperu,
        dag=dag,
        )
    
        
    process_data_osinergmin_precios_referencia = PythonOperator(
        task_id='process-data-osinergmin-referencia',
        python_callable=processDataPreciosReferencialesOsinergmin,
        dag=dag,
        )
    
    process_data_marcadores = PythonOperator(
        task_id='process-data-marcadores',
        python_callable=processDataMarcadores,
        dag=dag,
        )

    start_process = EmptyOperator(
        task_id='start-process',
        dag=dag,
        )
    
    end_process = EmptyOperator(
        task_id='end-process',
        dag=dag,
        )
    
    start_process >> process_data_minoristas>> process_data_combustibles_validos >> process_data_precios_mayorista_petroperu >> process_data_osinergmin_precios_referencia >> process_data_marcadores >> end_process