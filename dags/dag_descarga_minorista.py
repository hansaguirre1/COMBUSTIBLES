from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta



with DAG(
        'Descarga-data-minorista-diario',
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
        schedule_interval="0 10 * * *",
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
    
    process_data_minoristas = PythonOperator(
        task_id='process-data-minoristas',
        python_callable=processDataMinoristas,
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
    
    start_process >> process_data_minoristas>>  end_process