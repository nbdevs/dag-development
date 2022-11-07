from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator, BranchPythonOperator, ShortCircuitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from processor import Director, DatabaseETL, WarehouseETL
from colours import Colours

# Initializing global variables for duration of data collection 
start_date = 2017
end_date = 2022

# Instantiating classes used within the ETL process 
col = Colours()
db = DatabaseETL(col) # class responsible for database ETL
dw = WarehouseETL(col) # class responsible for warehouse ETL
db_director = Director(col, start_date, end_date, db, dw) # director composes objects which are loaded into database

# Creating functions to be used for dag callables
def load_db(self, db_director, decision, ti):
    return db.director.load_db(decision, ti)
   
# determining if incremental or full load is necessary 
def _determine_format(self, db_director, ti):
    return db.director.determine_format(ti)
 
# Defining baseline arguments for DAGs
default_args = {
    'start_date' : datetime(2022, 8, 1),
	'schedule_interval' : '@weekly',
	'catchup_by_default' : False,
    "do_xcom_push" : False
}

# Defining DAGs and tasks
with DAG(
    dag_id='database_etl',
    default_args = default_args,
    render_template_as_native_obj=True) as db_etl:

    determine_format = BranchPythonOperator(
        task_id = 'determine_extract_format',
        python_callable=_determine_format,
        dag=db_etl,
        sla=timedelta(minutes=)
    )
    
    with TaskGroup(group_id='bulk_load') as full_load:
        
        full_extract_load = PythonOperator(
            task_id = '',
            python_callable=load_db,
            op_kwargs={"decision" : 1, "ti" : "ti"},
            dag=db_etl,
            sla=timedelta(minutes=)
        )    
    
    with TaskGroup(group_id='incremental_load') as incremental_load:
        
        incremental_extract_load = PythonOperator(
            task_id = '',
            python_callable=load_db,
            op_kwargs={"decision" : 2, "ti" : "ti"},
            dag=db_etl,
            do_xcom_push = True,
            sla=timedelta(minutes=)
        )
    
   
    with TaskGroup(group_id='transform') as transform_full:
        
        transform_tables_full = PostgresOperator(task_id='',
                                                 sql='',
                                                 dag='',
                                                 sla=timedelta(minutes=))
   
        transform_joining_tables_full = PostgresOperator(task_id='',
                                                      sql='',
                                                      dag='',
                                                      sla=timedelta(minutes=))
        
        
    with TaskGroup(group_id='transform') as transform_inc:
        
        transform_tables_inc = PostgresOperator(task_id='',
                                                sql='',
                                                dag='',
                                                sla= timedelta(minutes=))
   
        transform_joining_tables_inc = PostgresOperator(task_id='',
                                                        sql='',
                                                        dag='',
                                                        sla= timedelta(minutes=))
        
    
    change_data_capture = ShortCircuitOperator(task_id='',
                                               python_callable='',
                                               dag='',
                                               op_kwargs={},
                                               op_args={},
                                               trigger_rule='',
                                               sla = timedelta(minutes=),
                                               )
    
    
    create_champ_views = PostgresOperator(task_id='',
                                          sql='',
                                          trigger_rule= '',
                                          dag='',
                                          sla = timedelta(minutes=)
                                          )
   
# Defining task dependencies 

_determine_format >> full_extract_load >> transform_tables_full >> transform_joining_tables_full >> create_champ_views
_determine_format >> incremental_extract_load >> change_data_capture >> transform_tables_inc >> transform_joining_tables_inc >> create_champ_views

