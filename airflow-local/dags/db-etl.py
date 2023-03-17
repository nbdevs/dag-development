from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator, BranchPythonOperator, ShortCircuitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models.param import Param

from processor import Director, DatabaseETL, WarehouseETL
from connections import PostgresClient, PostgresConnection
from colours import Colours

# Initializing global variables for duration of data collection
start_date = 2017
end_date = 2022

# Instantiating classes used within the ETL process
col = Colours()
db_handler = DatabaseETL(col)  # class responsible for database ETL
dw_handler = WarehouseETL(col)  # class responsible for warehouse ETL
# director composes objects which are loaded into database
db_director = Director(start_date, end_date, col, db_handler, dw_handler)
pg_conn = PostgresConnection(1)
client = PostgresClient()

# Creating functions to be used for dag callables

# determining if incremental or full load is necessary

def _retrieve_extract_type(db_handler, ti):
    """This function determines the extract format of the load cycle.
    Takes two arguments, the ETL class that directs the tasks within pipeline, and a reference to a task instance for pushing results
    to the airflow metadata database."""

    from decouple import config
    
    #path to file on linux machine 
    pathway = config("pathway")
    
    # accessing current context of running task instance

    extract_type = db_handler.determine_format(pathway)
    ti.xcom_push(key='extract_format', value=extract_type)

def _determine_format(ti) -> str:
    """This function determines the branch pathway to follow, determined by the retrieved xcoms 
    value."""

    # accessing current context of running task instance

    extract_type = ti.xcom_pull(key='extract_format', task_ids='retrieve_extract_type')
    
    if extract_type == 'Full':
        return 'full_extract_load'
    elif extract_type == 'Incremental':
        return 'incremental_extract_load'

# extract and load new data into database

def _load_db(db_director, ti):
    """This function calls the extract and load function to kick off the pipeline.
    Takes three arguments, the first being the ETL orchestration class as a param, the second being the decision of full load vs incremental load, and finally a 
    reference to the task instance to push the results to the airflow metadata database."""

    load_type = ti.xcom_pull(key='extract_format', task_ids='retrieve_extract_type')
    
    if load_type == 'Full':
        decision = 1
            
        # calling function from processor module 
        # dummy variables, these values will not be filled on this flow of control
        results_table, qualifying_table, race_telem_table, quali_telem_table = db_director.load_db(ti, decision)
 
        
    elif load_type == 'Incremental':
        decision = 2
        
        # calling function from processor module
        results_table, qualifying_table, race_telem_table, quali_telem_table = db_director.load_db(ti, decision)
        
        ti.xcom_push(key='results_table', value=results_table)
        ti.xcom_push(key='qualifying_table', value=qualifying_table)
        ti.xcom_push(key='race_telem_table', value=race_telem_table)
        ti.xcom_push(key='quali_telem_table', value=quali_telem_table)
        

# detecting new data
def _changed_data_capture(client, pg_conn, ti, db_handler):
    """This class is responsible for determining if there has been any changed data detected between new loaded data
    and that currently in the database tables. Takes two arguments, first being a reference to the postgres client class, and second being a reference to the postgres connection class."""
    import logging
    
    # accessing current context to pull tables from xcoms for serialisation 
    
    logging.info("Loading dataframes stored within xcomms...")
    qualifying_table = ti.xcom_pull(key='qualifying_table', task_ids='load_db')
    results_table = ti.xcom_pull(key='results_table', task_ids='load_db')
    race_telem_table = ti.xcom_pull(key='race_telem_table', task_ids='load_db')
    quali_telem_table = ti.xcom_pull(key='quali_telem_table', task_ids='load_db')
  
    # calling function needed to generate the extract date of the newly ingested data
    extract_dt = db_handler.increment_serialize(qualifying_table, results_table, race_telem_table, quali_telem_table, ti)
    
    # storing the extract_dt in xcoms for later retrieval
    ti.xcom_push(key='incremental_extract_dt', value=extract_dt)
    
    # creating the postgres conn uri by calling the function from the client.
    pg_conn_uri = client.get_connection_id(pg_conn)
    client.changed_data_capture(pg_conn_uri, extract_dt, results_table, qualifying_table, race_telem_table, quali_telem_table)
    
    return

def get_task_group(dag, group_id, default_args):
    """This function returns the task group for the full transform function"""

    with TaskGroup(group_id=group_id, default_args=default_args, dag=dag) as transform_full:

        transform_tables_full = PostgresOperator(task_id='transform_tables_full',
                                                 sql='CALL',
                                                 sla=timedelta(minutes=10))

        transform_joining_tables_full = PostgresOperator(task_id='transform_joining_tables_full',
                                                         sql='CALL',
                                                         sla=timedelta(minutes=10))

    return transform_full

def get_taskg(dag, group_id, default_args):
    """This function returns the task group for the transformation phase for the incremental pathway.
        Takes the name of the dag, the group_id it belongs to and default args as parameters for the function."""

    with TaskGroup(group_id=group_id, default_args=default_args, dag=dag) as transform_inc:

        transform_tables_inc = PostgresOperator(task_id='transform_tables_inc',
                                                sql='CALL',
                                                sla=timedelta(minutes=10))

        transform_joining_tables_inc = PostgresOperator(task_id='transform_joining_tables_inc',
                                                        sql='CALL',
                                                        sla=timedelta(minutes=10))
    return transform_inc

def _changed_data_detected(self, db_handler, ti):
    """ Function retrieves the xcoms values for the dataframes for race, result, qualifying, and telemetry data required in order to serialize the data 
    and upserts the data into the postgres database - this function is specific to the incremental flow of control. Also takes a reference to a DatabaseETL
    object and a task instance object."""
    
    import logging
    
    # accessing current context of running task instance to pull the tables which are to be compared to the existing database tables
    
    logging.info("Loading dataframes stored within xcomms...")
    qualifying_table = ti.xcom_pull(key='qualifying_table', task_ids='load_db')
    results_table = ti.xcom_pull(key='results_table', task_ids='load_db')
    race_telem_table = ti.xcom_pull(key='race_telem_table', task_ids='load_db')
    quali_telem_table = ti.xcom_pull(key='quali_telem_table', task_ids='load_db')
    
    #pushing the extract date into xcoms for later retrieval
    extract_dt = ti.xcom_pull(key='incremental_extract_dt', task_ids='changed_data_detected')
    
    # calling method from passed class object
    db_handler.changed_data_detected(ti, qualifying_table, results_table, race_telem_table, quali_telem_table, extract_dt)
    
    return

# Defining baseline arguments for DAGs
default_args = {
    'start_date': datetime(2023, 3, 4),
    'schedule_interval': '@weekly',
    'catchup_by_default': False,
    'do_xcom_push': True,
    'retries':1,
    'provide_context': True, 
    'retry_delay': timedelta(minutes=1)
}

# Defining DAGs and tasks
with DAG(
        dag_id='database_etl',
        default_args=default_args,
        render_template_as_native_obj=True,
        dagrun_timeout=timedelta(minutes=120)) as db_etl:
    
    retrieve_extract_type = PythonOperator(
        task_id='retrieve_extract_type',
        python_callable=_retrieve_extract_type,
        op_kwargs={"db_handler": db_handler},
        sla=timedelta(minutes=10),
        do_xcom_push=False
    )

    determine_extract_format = BranchPythonOperator(
        task_id='determine_extract_format',
        op_kwargs={"db_handler": db_handler},
        python_callable=_determine_format,
        sla=timedelta(minutes=5),
        do_xcom_push=False
    )

    full_extract_load = PythonOperator(
        task_id='full_extract_load',
        python_callable=_load_db,
        op_kwargs={"db_director": db_director},
        sla=timedelta(minutes=240)
    )

    incremental_extract_load = PythonOperator(
        task_id='incremental_extract_load',
        python_callable=_load_db,
        op_kwargs={"db_director": db_director},
        sla=timedelta(minutes=20)
    )

    change_data_capture = ShortCircuitOperator(task_id='change_data_capture',
                                               params={"pg_conn" : Param(pg_conn, type="object"), "client": Param(client, type="object")},
                                               python_callable=_changed_data_capture,
                                               sla=timedelta(minutes=20),
                                          
    )
    
    changed_data_detected = PythonOperator(task_id='changed_data_detected',
                                           params={"db_handler" : Param(db_handler, type="object")},
                                           python_callable=_changed_data_detected,
                                           sla=timedelta(minutes=15)
    )

    transform_full = get_task_group(db_etl, 'transform_full', default_args)

    transform_inc = get_taskg(db_etl, 'transform_incremental', default_args)

    create_champ_views = PostgresOperator(task_id='create_champ_views',
                                          sql='CALL',
                                          dag=db_etl,
                                          sla=timedelta(minutes=30))

# Defining task dependencies

retrieve_extract_type >> determine_extract_format >> full_extract_load >> transform_full >> create_champ_views 
retrieve_extract_type >> determine_extract_format >> incremental_extract_load >> change_data_capture >> changed_data_detected >> transform_inc >> create_champ_views 