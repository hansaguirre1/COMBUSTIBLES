from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta


with DAG(
        'Procesar-data-combustibles',
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
        schedule_interval="0 6 * * *",
        start_date=datetime(2021, 1, 1, 10, 15),
        catchup=False,
) as dag:
    
    def processDataPetroperu():
        from src.injection.containers import Container
        from src.domain.repositories.remote_repository import RemoteRepository
        from src.domain.repositories.petroperu_repository import PetroperuRepository
        from src.config.get_env import url_petroperu

        container = Container()
        remoteRepository: RemoteRepository = container.remote_repository()
        petroperuRepository: PetroperuRepository = container.petroperu_repository()
        
        remoteRepository.getDataPetroperu(url=url_petroperu)
        petroperuRepository.saveData()
    
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
    
    
    def getDataOsinergmin():
        from src.domain.repositories.remote_repository import RemoteRepository
        from src.domain.repositories.referencia_repository import ReferenciaRepository

        from src.config.get_env import url_osinergmin
        from src.injection.containers import Container
        
        container = Container()
        remoteRepository: RemoteRepository = container.remote_repository()
        referenciaRepository: ReferenciaRepository = container.referencia_repository()
        remoteRepository.getDataOsinergmin(url=url_osinergmin)
        referenciaRepository.saveDataReferencia()
    
    def processDataMinoristas():
        from src.injection.containers import Container
        from src.domain.repositories.remote_repository import RemoteRepository
        from src.domain.repositories.minorista_repository import MinoristaRepository
        
        from src.config.get_env import url_signeblock

        container = Container()
        remoteRepository: RemoteRepository = container.remote_repository()
        minoristaRepository: MinoristaRepository = container.minorista_repository()
        
        remoteRepository.getDataMinorista(url=url_signeblock)
        minoristaRepository.saveDataBase()
    
    def processDtaMinoristas():
        from src.injection.containers import Container
        from src.domain.repositories.minorista_repository import MinoristaRepository
        container = Container()
        minoristaRepository: MinoristaRepository = container.minorista_repository()
        
        minoristaRepository.saveDataToDta()
    
    def startProcess():
        from src.injection.containers import Container
        
        container = Container()
        db = container.db()
        db.create_database()
    
    start_process = PythonOperator(
        task_id='start-process',
        python_callable=startProcess,
        dag=dag,
        )
    
    process_data_petroperu = PythonOperator(
        task_id='process-data-petroperu',
        python_callable=processDataPetroperu,
        dag=dag,
        )
    
    process_data_marcadores = PythonOperator(
        task_id='process-data-marcadores',
        python_callable=processDataMarcadores,
        dag=dag,
        )
    
    process_data_osinergmin_referencia = PythonOperator(
        task_id='process-data-osinergmin-referencia',
        python_callable=getDataOsinergmin,
        dag=dag,
        )
    
    process_data_signeblock = PythonOperator(
        task_id='process-data-minoristas',
        python_callable=processDataMinoristas,
        dag=dag,
        )
    
    process_final_dta = PythonOperator(
        task_id='save-final-dta-minoritas',
        python_callable=processDataMinoristas,
        dag=dag,
        )

    end_process = EmptyOperator(task_id='end-process-data')
    
    # start_process >> remote_data_petroperu >> remote_data_marcadores >> remote_data_osinergmin >> remote_data_signeblock >> end_process
    start_process >> process_data_marcadores >> process_data_signeblock >> process_final_dta >> process_data_petroperu >> process_data_osinergmin_referencia >> end_process