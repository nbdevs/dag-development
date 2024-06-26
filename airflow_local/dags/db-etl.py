from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator, ShortCircuitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Pool

from processor import DatabaseETL, WarehouseETL
from director import Director
from colours import Colours
 
# Initializing global variables for duration of data collection
start_date = 2022 # telemetry data only available from 2019 onwards for races
end_date = 2024

# Instantiating classes used within the ETL process
col = Colours()
db_handler = DatabaseETL(col)  # class responsible for database ETL
dw_handler = WarehouseETL(col)  # class responsible for warehouse ETL
# director composes objects which are loaded into database
db_director = Director(start_date, end_date, col, db_handler, dw_handler)

# Defining baseline arguments for DAGs
default_args = {
    'start_date': datetime(2024, 5, 12),
    'schedule_interval': 'None', # change this back to weekly after 
    'catchup_by_default': False,
    'do_xcom_push': True,
    'retries': 1,
    'provide_context': True, 
    'retry_delay': timedelta(minutes=1),
    'owner': 'airflow',
    'queue': 'rabbit.db'
}

# Create pool for telemetry granularity
telemetry_pool= Pool.create_or_update_pool(
    "telemetry_pool",
    slots=128,
    description="Pool for telemetry loading tasks.",
    include_deferred=True
)

# Defining DAGs and tasks
with DAG(
        dag_id='database_etl',
        default_args=default_args,
        render_template_as_native_obj=True,
        dagrun_timeout=timedelta(minutes=1000)) as db_etl:
    
    retrieve_extraction_type = PythonOperator(
        task_id='retrieve_extract_type',
        python_callable=db_director.retrieve_extract_type,
        sla=timedelta(minutes=1),
        do_xcom_push=False
    )

    determine_extraction_format = BranchPythonOperator(
        task_id='determine_extract_format',
        python_callable=db_director.determine_format,
        sla=timedelta(minutes=2),
        do_xcom_push=False
    )
    
    # full load task groups 
    full_extraction_load_season = db_director.full_season_load()
    
    full_extraction_load_race = db_director.full_load_race(db_etl, 'full_ext_load_race', default_args)
    
    # full_extraction_load_telemetry = db_director.full_load_telemetry() # resource heavy tasks using kubernetespodoperator 

    full_extraction_load_pre_transf = db_director.full_load_pre_transformation()
    
    # incremental load task groups 
    incremental_extraction_load_race = db_director.inc_load_race(db_etl, 'incremental_ext_load_race', default_args)
    
    # incremental_extraction_load_telem = db_director.inc_load_telem() # resource heavy tasks using kubernetespodoperator 
    
    incremental_extraction_load_pre_transf = db_director.inc_load_pre_transf()

    change_data_capture = ShortCircuitOperator(task_id='change_data_capture',
                                               python_callable=db_director.changed_data_handler,
                                               sla=timedelta(minutes=20)                        
    )
    
    changed_data_detected = PythonOperator(task_id='changed_data_detected',
                                           python_callable=db_director.changed_data_detected,
                                           sla=timedelta(minutes=15)
    )

    full_transformation = db_director.full_trans(db_etl, 'transform_full', default_args)

    incremental_transformation = db_director.inc_trans(db_etl, 'transform_incremental', default_args)

    create_champ_views = PostgresOperator(task_id='create_champ_views',
                                          sql='CALL',
                                          dag=db_etl,
                                          sla=timedelta(minutes=30),
                                          trigger_rule='none_failed')

# Defining task dependencies

retrieve_extraction_type >> determine_extraction_format >> full_extraction_load_season >> full_extraction_load_race >> full_extraction_load_pre_transf >> full_transformation >> create_champ_views 
retrieve_extraction_type >> determine_extraction_format >> incremental_extraction_load_race >> incremental_extraction_load_pre_transf >> change_data_capture >> changed_data_detected >> incremental_transformation >> create_champ_views