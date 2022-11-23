from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
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
db = DatabaseETL(col)  # class responsible for database ETL
dw = WarehouseETL(col)  # class responsible for warehouse ETL
# director composes objects which are loaded into database
db_director = Director(col, start_date, end_date, db, dw)
pg_conn = PostgresConnection(1)
client = PostgresClient()

# Creating functions to be used for dag callables

# extract and load new data into database

def load_db(self, db_director, decision, ti):
    """This function calls the extract and load function to kick off the pipeline.
    Takes three arguments, the first being the ETL orchestration class as a param, the second being the decision of full load vs incremental load, and finally a 
    reference to the task instance to push the results to the airflow metadata database."""

    db.director.load_db(decision, ti)
    return

# determining if incremental or full load is necessary

def _determine_format(self, db_director, ti):
    """This function determines the extract format of the load cycle.
    Takes two arguments, the ETL class that directs the tasks within pipeline, and a reference to a task instance for pushing results
    to the airflow metadata database."""

    db_director.determine_format(ti)
    return

# detecting new data

def _changed_data_capture(self, client, pg_conn):
    """This class is responsible for determining if there has been any changed data detected between new loaded data
    and that currently in the database tables. Takes two arguments, first being a reference to the postgres client class, and second being a reference to the postgres connection class."""

    # creating the postgres conn uri by calling the function from the client.
    pg_conn_uri = client.get_connection_id(pg_conn)
    client.changed_data_capture(pg_conn_uri)
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

def _changed_data_detected(self, db_director, ti):
    """ Function retrieves the xcomms values for the dataframes for race, result, qualifying, and telemetry data required in order to serialise the data 
    and upserts into the postgres database - this function is specific to the incremental flow of control. Also takes a reference to a DatabaseETL
    object and a task instance object."""
    
    # calling method from passed class object
    db_director.changed_data_detected(ti)
    return

# Defining baseline arguments for DAGs
default_args = {
    'start_date': datetime(2022, 8, 1),
    'schedule_interval': '@weekly',
    'catchup_by_default': False,
    "do_xcom_push": False
}

# Defining DAGs and tasks
with DAG(
        dag_id='extract_load_etl',
        default_args=default_args,
        render_template_as_native_obj=True) as db_etl:

    determine_format = BranchPythonOperator(
        task_id='determine_extract_format',
        params={"db_director" : Param(db_director, type="object")},
        python_callable=_determine_format,
        do_xcom_push=True,
        sla=timedelta(minutes=5)
    )

    full_extract_load = PythonOperator(
        task_id='full_extract_load',
        python_callable=load_db,
        op_kwargs={"decision": 1},
        do_xcom_push=True,
        sla=timedelta(minutes=240)
    )

    incremental_extract_load = PythonOperator(
        task_id='incremental_extract_load',
        python_callable=load_db,
        op_kwargs={"decision": 2},
        do_xcom_push=True,
        sla=timedelta(minutes=20)
    )

    change_data_capture = ShortCircuitOperator(task_id='change_data_capture',
                                               params={"pg_conn" : Param(pg_conn, type="object"), "client": Param(client, type="object")},
                                               python_callable=_changed_data_capture,
                                               sla=timedelta(minutes=20),
                                               do_xcom_push=True
    )
    
    changed_data_detected = PythonOperator(task_id='changed_data_detected',
                                           params={"db_director" : Param(db_director, type="object")},
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

determine_format >> full_extract_load
determine_format >> incremental_extract_load >> change_data_capture >> changed_data_detected
