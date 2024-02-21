from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow.sensors.external_task import ExternalTaskSensor


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
        schedule_interval="0 12 * * 1",
        start_date=datetime(2021, 1, 1, 10, 15),
        catchup=False,
) as dag:
    

    
    def processDataPreciosMayorista():
        from src.injection.containers import Container
        from src.domain.repositories.remote_repository import RemoteRepository
        from src.domain.repositories.precios_mayoristas_repository import PreciosMayoristasRepository

        container = Container()
        remoteRepository: RemoteRepository = container.remote_repository()
        preciosMayoristasRepository: PreciosMayoristasRepository = container.precios_mayoristas_repository()
        
        remoteRepository.m0_descarga_mayorista()
        preciosMayoristasRepository.saveDataMayoristas()
           
    
    def processDataUbigeo():
        from src.config.get_env import url_bcrp, url_eia
        from src.injection.containers import Container
        from src.domain.repositories.ubigeo_repository import UbigeoRepository

        container = Container()
        ubigeoRepository: UbigeoRepository = container.ubigeo_repository()
        
        ubigeoRepository.saveDataUbigeo()
    
    
    def processDataMinoristas():
        from src.injection.containers import Container
        from src.domain.repositories.remote_repository import RemoteRepository
        from src.domain.repositories.minorista_repository import MinoristaRepository
        
        from src.config.get_env import url_signeblock

        container = Container()
        remoteRepository: RemoteRepository = container.remote_repository()
        minoristaRepository: MinoristaRepository = container.minorista_repository()
        
        # remoteRepository.getDataMinorista(url=url_signeblock)
        minoristaRepository.saveDataBase()
    
    def processDataLatLngMayorista():
        from src.injection.containers import Container
        from src.domain.repositories.lat_lng_mayorista_repository import LatLngMayoristaRepository

        container = Container()
        latLngMinoristaRepository: LatLngMayoristaRepository = container.lat_lng_mayoristas_repository()
        
        latLngMinoristaRepository.saveDataLatLng()
    
    # def processIndicadorMinorista():
    #     from src.injection.containers import Container
    #     from src.domain.repositories.indicador_repository import IndicadorRepository
    #     container = Container()
    #     indicadorRepository: IndicadorRepository = container.indicador_repository()
    #     indicadorRepository.saveAndProcessData()
    
    def processDtaMinoristas():
        from src.injection.containers import Container
        from src.domain.repositories.minorista_repository import MinoristaRepository
        container = Container()
        minoristaRepository: MinoristaRepository = container.minorista_repository()
        
        # minoristaRepository.saveDataToDta()
    
    def startProcess():
        from src.injection.containers import Container
        
        container = Container()
        db = container.db()
        db.create_database()
    
    # listening_process = ExternalTaskSensor(
    #     task_id='listening_process',
    #     external_dag_id="Descarga-data-minorista-diario",
    #     dag=dag,
    #     )
    
    start_process = PythonOperator(
        task_id='start-process',
        python_callable=startProcess,
        dag=dag,
        )
    
    process_lat_lng_mayorista_task = PythonOperator(
        task_id='process_lat_lng_mayorista',
        python_callable=processDataLatLngMayorista,
        dag=dag,
        )
    
    process_data_ubigeo = PythonOperator(
        task_id='process_data_ubigeo',
        python_callable=processDataUbigeo,
        dag=dag,
        )


    
    process_precio_mayorista= PythonOperator(
        task_id='process_precio_mayorista',
        python_callable=processDataPreciosMayorista,
        dag=dag,
        )
    
    process_data_minoristas = PythonOperator(
        task_id='process-data-minoristas',
        python_callable=processDataMinoristas,
        dag=dag,
        )
    
    # process_final_dta = PythonOperator(
    #     task_id='save-final-dta-minoritas',
    #     python_callable=processDataMinoristas,
    #     dag=dag,
    #     )

    end_process = EmptyOperator(task_id='end-process-data')
    
    # start_process >> remote_data_petroperu >> remote_data_marcadores >> remote_data_osinergmin >> remote_data_signeblock >> end_process
    start_process>> process_data_minoristas >> process_data_ubigeo >> process_precio_mayorista   >> process_lat_lng_mayorista_task >> end_process