from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator, BranchPythonOperator, ShortCircuitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from processor import Director, DatabaseETL, WarehouseETL
from connections import PostgresClient, PostgresConnection
from colours import Colours

# Initializing global variables for duration of data collection 
start_date = 2017
end_date = 2022

# Instantiating classes used within the ETL process 
col = Colours()
db = DatabaseETL(col) # class responsible for database ETL
dw = WarehouseETL(col) # class responsible for warehouse ETL
db_director = Director(col, start_date, end_date, db, dw) # director composes objects which are loaded into database
pg_conn = PostgresConnection(1)
client = PostgresClient()

# Creating functions to be used for dag callables

# extract and load new data into database
def load_db(self, db_director, decision, ti):
    """This function calls the extract and load function to kick off the pipeline.
    Takes three arguments, the first being the ETL orchestration class as a param, the second being the decision of full load vs incremental load, and finally a 
    reference to the task instance to push the results to the airflow metadata database."""
    return db.director.load_db(decision, ti)
   
# determining if incremental or full load is necessary 
def _determine_format(self, db_director, ti):
    """This function determines the extract format of the load cycle.
    Takes two arguments, the ETL class that directs the tasks within pipeline, and a reference to a task instance for pushing results
    to the airflow metadata database."""
    
    return db.director.determine_format(ti)

# detecting new data 
def _changed_data_capture(self, client, pg_conn):
    """This class is responsible for determining if there has been any changed data detected between new loaded data
    and that currently in the database tables. Takes two arguments, first being a reference to the postgres client class, and second being a reference to the postgres connection class."""
    
    # creating the postgres conn uri by calling the function from the client. 
    pg_conn_uri = client.get_connection_id(pg_conn)
    return client.change_data_capture(pg_conn_uri)
    
def get_task_group(dag, group_id, default_args):
    
    with TaskGroup(group_id=group_id, default_args = default_args, dag=dag) as transform_full:
    
        transform_tables_full = PostgresOperator(task_id='transform_tables_full',
                                                sql='CALL',
                                                sla=timedelta(minutes=10))

        transform_joining_tables_full = PostgresOperator(task_id='transform_joining_tables_full',
                                                    sql='CALL',
                                                    sla=timedelta(minutes=10))

    return transform_full

def get_taskg(dag, group_id, default_args):
    
    with TaskGroup(group_id=group_id, default_args = default_args, dag = dag) as transform_inc:
        
        transform_tables_inc = PostgresOperator(task_id='transform_tables_inc',
                                                sql='CALL',
                                                sla= timedelta(minutes=10))
   
        transform_joining_tables_inc = PostgresOperator(task_id='transform_joining_tables_inc',
                                                        sql='CALL',
                                                        sla= timedelta(minutes=10))
    return transform_inc

 
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
        sla=timedelta(minutes=5)
    )
        
    full_extract_load = PythonOperator(
        task_id = 'full_extract_load',
        python_callable=load_db,
        op_kwargs={"decision" : 1, "ti" : "ti"},
        do_xcom_push = True,
        sla=timedelta(minutes=240)
    )    

    incremental_extract_load = PythonOperator(
        task_id = 'incremental_extract_load',
        python_callable=load_db,
        op_kwargs={"decision" : 2, "ti" : "ti"},
        do_xcom_push = True,
        sla=timedelta(minutes=20)
    )
    
    change_data_capture = ShortCircuitOperator(task_id='change_data_capture',
                                               python_callable=_changed_data_capture,
                                               dag=db_etl,
                                               op_kwargs={"client": "client", "pg_conn" : "pg_conn"},
                                               sla = timedelta(minutes=20),
                                               do_xcom_push=True
                                               )
    
    transform_full = get_task_group(db_etl, 'transform_full', default_args)
    
    transform_inc = get_taskg(db_etl, 'transform_incremental', default_args)
        
    create_champ_views = PostgresOperator(task_id='create_champ_views',
                                          sql='CALL',
                                          dag=db_etl,
                                          sla = timedelta(minutes=30)
                                          )
   
# Defining task dependencies 
 
_determine_format >> full_extract_load >> transform_full >> create_champ_views
_determine_format >> incremental_extract_load >> change_data_capture >> transform_inc >> create_champ_views

