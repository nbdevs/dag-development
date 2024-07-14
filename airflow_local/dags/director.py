from airflow import DAG
class Director:
    """ This class coordinates the complete ETL pipeline, from API to DB to DW.
    Two functions for each process to upsert into database, and to effective load the data into data stores and then call 
    a stored procedure for the SQL (postgres, snowsql) transformations."""

    import pandas as pd

    def __init__(self, startPeriod, endPeriod, col, db_processor, dw_processor) -> pd.DataFrame:

        from decouple import config
        from connections import PostgresClient, SnowflakeClient, S3Client, PostgresConnection

        # instantiates the processor class to implement its functions
        self._db_builder = db_processor
        # instantiates the processor class to implement its functions
        self._dw_builder = dw_processor
        self._postgres = PostgresClient()  # conn string for db dev or dw dev
        self._snowflake = SnowflakeClient()  # snowflake client connection details
        self._pg_conn = PostgresConnection(1)  # postgres connection class
        self._s3_storage = S3Client()  # s3 client connection details
        self._start_date = startPeriod  # start period for season of formula 1 data
        self._end_date = endPeriod  # end period for season of formula 1 data
        self._col = col  # A colour formatting class
        # env variable for directory to store cache for full load season
        self._fl_season_dir = config("season")
        # env variable for directory to store cache for full load results
        self._fl_results_dir = config("results")
        # env variable for directory to store cache for full qualifying
        self._fl_qualifying_dir = config("qualifying")
        # env variable for directory to store cache for full load telemetry
        self._fl_telem_dir = config("telemetry")
        # env variable for directory to store cache for results
        self._inc_results_dir = config("inc_results")
        # env variable for directory to store cache for incremental qualifying
        self._inc_qualifying_dir = config("inc_qualifying")
        # env variable for directory to store cache for incremental quali telemetry
        self._inc_qualitelem_dir = config("inc_qualitelem")
        # env variable for directory to store cache for incremental race telemetry
        self._inc_racetelem_dir = config("inc_racetelem")
        # stores the csv files for full load
        self._full_processed_dir = config("full_processed_dir")
        # stores the csv files for incremental load
        self._inc_processed_dir = config("inc_processed_dir")
        self._pathway = config("pathway")
        self._user_home_dir = config("user_home")
        self._file_array = ["Results", "Season", "Qualifying", "Telemetry"]

    def retrieve_extract_type(self, ti):
        """This function determines the extract format of the load cycle.
        Takes two arguments, the ETL class that directs the tasks within pipeline, and a reference to a task instance for pushing results
        to the airflow metadata database."""

        # determining if incremental or full load is necessary
        extract_type = self._db_builder.determine_format(self._pathway)
        # accessing current context of running task instance
        ti.xcom_push(key='extract_format', value=extract_type)

        return

    def determine_format(self, ti) -> str:
        """This function determines the branch pathway to follow, determined by the retrieved xcoms 
        value."""

        # accessing current context of running task instance
        extract_type = ti.xcom_pull(
            key='extract_format', task_ids='retrieve_extract_type')

        if extract_type == 'Full':
            # path to taskgroup task
            return 'full_season_load'

        elif extract_type == 'Incremental':

            # string lists of tasks
            tasks = ['incremental_qualifying_load', 'incremental_results_load']
            task_group = []
            size = len(tasks)

            # path to taskgroup task
            for i in range(0, size):
                task_group.append(
                    'incremental_ext_load_race.{}'.format(tasks[i]))

            return task_group

    def full_load_season(self):
        """ This function generates the tabled data for the season level of granularity for F1 races.
        Takes two inputs, one being the db_handler which is a reference to the processor class, and 
        a ti argument for a reference to the task instance of the current running instance"""

        import logging
        from datetime import datetime

        # variable initialisation
        extract_dt = datetime.today().strftime("%Y-%m-%d")  # date stamp for record keeping 

        logging.info("Extracting Aggregated Season Data...")
        # outputs aggregated season data as dataframe to be pushed to xcoms
        season_table = self._db_builder.extract_season_grain(
            self._fl_season_dir, self._start_date, self._end_date)
        
        logging.info("Serialising dataframe to CSV...")
        self._db_builder.csv_producer(self._user_home_dir, extract_dt, season_table, "Season", "Full") # function call to store csv file of dataframe content

        return

    def full_load_qualifying(self):
        """ This function generates the tabled data for the race level of granularity for F1 races.
        Takes two inputs, one being the db_handler which is a reference to the processor class, and 
        a ti argument for a reference to the task instance of the current running instance"""

        import logging
        from datetime import datetime

        # variable initialisation
        extract_dt = datetime.today().strftime("%Y-%m-%d")  # date stamp for record keeping 

        logging.info("Extracting Aggregated Qualifying Data...")
        # outputs aggregated qualifying data as dataframe to be pushed to xcoms
        qualifying_table = self._db_builder.extract_qualifying_grain(
            self._fl_qualifying_dir, self._start_date, self._end_date)
        logging.info("Serialising dataframe to CSV...")
        try:
            self._db_builder.csv_producer(self._user_home_dir, extract_dt, qualifying_table, "Qualifying", "Full") # function call to store csv file of dataframe content
        except Exception as e:
            logging.error(e)
            
        return

    def full_load_results(self, ti):
        """ This function generates the tabled data for the race level of granularity for F1 races.
        Takes two inputs, one being the db_handler which is a reference to the processor class, and 
        a ti argument for a reference to the task instance of the current running instance"""

        import logging
        from datetime import datetime
 
        # variable initialisation
        extract_dt = datetime.today().strftime("%Y-%m-%d")  # date stamp for record keeping 

        logging.info("Extracting Aggregated Results Data...")
        # outputs dataframe containing aggregated results table which will be pushed to xcom
        results_table = self._db_builder.extract_race_grain(
            self._fl_results_dir, self._start_date, self._end_date)
        logging.info("Serialising dataframe to CSV...")
        try:
            self._db_builder.csv_producer(self._user_home_dir, extract_dt, results_table, "Results", "Full") # function call to store csv file of dataframe content
        except Exception as e:
            logging.error(e)
        
        # pushing extract date value to xcoms
        ti.xcom_push(key='extract_date', value=extract_dt)

        return

    def full_load_race_telem(self):
        """ This function generates the tabled data for the telemetry level of granularity for F1 races.
        Takes two inputs, one being the db_handler which is a reference to the processor class, and 
        a ti argument for a reference to the task instance of the current running instance"""

        import logging
        from datetime import datetime

        # variable initialisation
        extract_dt = datetime.today().strftime("%Y-%m-%d")  # date stamp for record keeping 

        logging.info("Extracting Aggregated Race Telemetry Data...")

        # outputs dataframe containing all the aggregated race telem data to be outputted to xcoms.
        race_telem_table = self._db_builder.extract_race_telem(
            self._fl_telem_dir, self._start_date, self._end_date)
        logging.info("Serialising dataframe to CSV...")
        try:
            self._db_builder.csv_producer(self._user_home_dir, extract_dt, race_telem_table, "RaceTelemetry", "Full") # function call to store csv file of dataframe content
        except Exception as e:
            logging.error(e)

        return 

    def full_load_quali_telem(self, ti):
        """ This function generates the tabled data for the telemetry level of granularity for F1 races.
        Takes two inputs, one being the db_handler which is a reference to the processor class, and 
        a ti argument for a reference to the task instance of the current running instance"""

        import logging
        from datetime import datetime
 
        # variable initialisation
        extract_dt = datetime.today().strftime("%Y-%m-%d")  # date stamp for record keeping 
   
        logging.info("Extracting Aggregated Qualifying Telemetry Data...")

        # outputs the dataframe for the qualifying data
        quali_telem_table = self._db_builder.extract_quali_telem(
            self._fl_telem_dir, self._start_date, self._end_date)
        
        logging.info("Serialising dataframe to CSV...")
        
        try:
            self._db_builder.csv_producer(self._user_home_dir, extract_dt, quali_telem_table, "QualifyingTelemetry", "Full") # function call to store csv file of dataframe content
        except Exception as e:
            logging.error(e)
        
        # pushing extract date value to xcoms
        ti.xcom_push(key='extract_date', value=extract_dt)

        return 

    def full_load_upsert(self, ti):
        """ This function upserts the CSV data into the staging  database tables ready to be transformed into the production 3nf tables. 
        Takes two inputs, one being the db_handler which is a reference to the processor class, and 
        a ti argument for a reference to the task instance of the current running instance"""

        import logging
        
        # retrieving the extract_date of the last load from xcom
        extract_dt = ti.xcom_pull(task_ids='full_ext_load_race.full_results_load', key='extract_date')

        #retrieve load_type 
        load_type = ti.xcom_pull(task_ids='retrieve_extract_type', key="extract_format")
        
        logging.info("Upserting data into Postgres...")

        # initializing postgres client to generate postgres connection uri
        # calling connection method to get connection string with 1 specified for the database developer privileges
        pg_conn_uri = self._postgres.connection_factory(1, self._col)
        self._postgres.upsert_db(pg_conn_uri, self._inc_processed_dir, self._full_processed_dir, extract_dt, load_type)
        
        # clean up cache folder 
        pathway = 'cache'
        logging.info("[CACHE CLEANUP] Beginning cleaning post-process...")
        home_path_cache = self._user_home_dir + f'/{pathway}'
        home_dir = [home_path_cache, ""]
        self._db_builder.cache_cleaner(load_type, home_dir[0], "cloud", pathway)
        logging.info("[CACHE CLEANUP] Cleanup post-process completed.")

        return

    def inc_qualifying(self, ti):
        """ This function generates the tabled data for the race level of granularity for F1 races.
        Takes two inputs, one being the db_handler which is a reference to the processor class, and 
        a ti argument for a reference to the task instance of the current running instance"""

        import logging
        from datetime import datetime
 
        # variable initialisation
        extract_dt = datetime.today().strftime("%Y-%m-%d")  # date stamp for record keeping 

        logging.info("Extracting Aggregated Qualifying Data.")

        # outputs the dataframe for the qualifying data to be stored in xcoms
        qualifying_table = self._db_builder.incremental_qualifying(
            ti, self._inc_qualifying_dir)
        
        logging.info("Serialising dataframe to CSV...")
        try:
            self._db_builder.csv_producer(self._user_home_dir, extract_dt, qualifying_table, "Qualifying", "Incremental") # function call to store csv file of dataframe content
        except Exception as e:
            logging.error(e)
        
        return

    def inc_results(self, ti):
        """ This function generates the tabled data for the race level of granularity for F1 races.
        Takes two inputs, one being the db_handler which is a reference to the processor class, and 
        a ti argument for a reference to the task instance of the current running instance"""

        import logging
        from datetime import datetime
 
        # variable initialisation
        extract_dt = datetime.today().strftime("%Y-%m-%d")  # date stamp for record keeping 
   
        logging.info("Extracting Aggregated Results Data...")

        # outputs the dataframe for the race result data to be stored in xcoms
        results_table = self._db_builder.incremental_results(ti, self._inc_results_dir)
        logging.info("Serialising dataframe to CSV...")
        try:
            self._db_builder.csv_producer(self._user_home_dir, extract_dt, results_table, "Results", "Incremental") # function call to store csv file of dataframe content
        except Exception as e:
            logging.error(e)
            
        # pushing extract date value to xcoms
        ti.xcom_push(key='incremental_extract_date', value=extract_dt)
                
        return

    def inc_quali_telem(self, start_date, end_date):
        """ This function generates the tabled data for the telemetry level of granularity for F1 races.
        Takes two inputs, one being the db_handler which is a reference to the processor class, and 
        a ti argument for a reference to the task instance of the current running instance"""

        import logging
        from datetime import datetime
 
        # variable initialisation
        extract_dt = datetime.today().strftime("%Y-%m-%d")  # date stamp for record keeping 
   
        logging.info("Extracting aggregated Quali Telemetry data... ")

        # outputs the dataframe for the qualifying telemetry data to be stored in xcoms
        quali_telem_table = self._db_builder.incremental_quali_telem(
            self._inc_quali_dir, start_date, end_date)
        logging.info("Serialising dataframe to CSV...")
        try:
            self._db_builder.csv_producer(self._user_home_dir, extract_dt, quali_telem_table, "QualifyingTelemetry", "Incremental") # function call to store csv file of dataframe content
        except Exception as e:
            logging.error(e)
            
        return 

    def inc_race_telem(self, ti):
        """ This function generates the tabled data for the telemetry level of granularity for F1 races.
        Takes two inputs, one being the db_handler which is a reference to the processor class, and 
        a ti argument for a reference to the task instance of the current running instance"""

        import logging
        from datetime import datetime
 
        # variable initialisation
        extract_dt = datetime.today().strftime("%Y-%m-%d")  # date stamp for record keeping 
   
        logging.info("Extracting aggregated Race Telemetry data... ")

        # outputs the dataframe for the race telemetry data to be stored in xcoms
        race_telem_table = self._db_builder.incremental_race_telem(
            ti, self._inc_racetelem_dir)
        logging.info("Serialising dataframe to CSV...")
        try:
            self._db_builder.csv_producer(self._user_home_dir, extract_dt, race_telem_table, "RaceTelemetry", "Incremental") # function call to store csv file of dataframe content
        except Exception as e:
            logging.error(e)
            
        return 

    def inc_load_upsert(self, ti):
        """ This function upserts the CSV data into the staging  database tables ready to be transformed into the production 3nf tables. 
        Takes two inputs, one being the db_handler which is a reference to the processor class, and 
        a ti argument for a reference to the task instance of the current running instance"""

        import logging
        
        determinant = self._db_builder.determine_load_date(self._pathway) # function call for determining load_date
        
        if determinant == "Full":
            extract_dt = ti.xcom_pull(task_ids='full_ext_load_race.full_qualifying_load', key='full_extract_date')# retrieving the extract_date of the last load from xcom
        elif determinant == "Incremental":
            extract_dt = ti.xcom_pull(task_ids="inc_ext_load_race.incremental_results_load", key="incremental_extract_date")# retrieving the extract_date of the last load from xcom
            
        # retrieve load_type 
        load_type = ti.com_pull(task_ids='retrieve_extract_type', key="extract_format")

        logging.info("Upserting data into Postgres...")
        
        # initializing postgres client to generate postgres connection uri
        # calling connection method to get connection string with 1 specified for the database developer privileges
        pg_conn_uri = self._postgres.connection_factory(1, self._col)
        self._postgres.upsert_db(pg_conn_uri, ti, self._inc_processed_dir, self._full_processed_dir, extract_dt, load_type)
        
        # clean up cache folder 
        pathway = "cache"
        home_path_cache = self._user_home_dir + f'/{pathway}'
        home_dir = [home_path_cache,""]
        logging.info("[CACHE CLEANUP] Beginning cleaning post-process...")
        home_dir = [home_path_cache, ""]
        self._db_builder.cache_cleaner(load_type, home_dir[0], "cloud", pathway)
        logging.info("[CACHE CLEANUP] Cleanup post-process completed.")
 
        return

    # extract and load new data into database

    def full_season_load(self):
        """This function calls the extract and load function to kick off the pipeline.
        Takes three arguments, the first being the ETL orchestration class as a param, the second being the decision of full load vs incremental load, and finally a 
        reference to the task instance to push the results to the airflow metadata database."""

        import logging
        from datetime import timedelta
        from airflow.operators.python import PythonOperator

        logging.info(
            "-----------------------------------Data extraction, cleaning and conforming-----------------------------------------")

        full_load_season = PythonOperator(task_id='full_season_load',
                                          python_callable=self.full_load_season,
                                          do_xcom_push=False,
                                          sla=timedelta(minutes=100))

        return full_load_season

    def full_load_race(self, dag, group_id, default_args):
        """This function calls the extract and load function to kick off the pipeline.
        Takes three arguments, the first being the ETL orchestration class as a param, the second being the decision of full load vs incremental load, and finally a 
        reference to the task instance to push the results to the airflow metadata database."""

        from datetime import timedelta
        from airflow.utils.task_group import TaskGroup
        from airflow.operators.python import PythonOperator

        with TaskGroup(group_id=group_id, default_args=default_args, dag=dag) as full_load_race:

            full_load_qualifying = PythonOperator(task_id='full_qualifying_load',
                                                  python_callable=self.full_load_qualifying,
                                                  do_xcom_push=False,
                                                  sla=timedelta(minutes=100))

            full_load_results = PythonOperator(task_id='full_results_load',
                                               python_callable=self.full_load_results,
                                               do_xcom_push=False,
                                               sla=timedelta(minutes=100))
            return full_load_race

    '''
    def full_load_telemetry(self):
        """This function calls the extract and load function to kick off the pipeline.
        Takes three arguments, the first being the ETL orchestration class as a param, the second being the decision of full load vs incremental load, and finally a 
        reference to the task instance to push the results to the airflow metadata database."""

        import os
        from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
        from kubernetes.client import models as k8s
        
                
        full_load_telem = KubernetesPodOperator( 
            namespace="default",
            image="nbroj/airflow-images:airflow-schedulerv1.0.0",
            image_pull_secrets=[],
            image_pull_policy='Never',
            task_id="full_load_telemetry",
            config_file=os.path.expanduser('~')+"/.kube/config",
            env_vars={'load_type': ```{{ti.xcom_pull(task_ids='determine_extract_format', key='extract_format')}}```
            },
            do_xcom_push="no",
            on_finish_action="delete_pod",
            in_cluster= False,
            get_logs=True,
            deferrable=True
        )
        return full_load_telem
    '''

    def full_load_pre_transformation(self):
        """This function calls the extract and load function to kick off the pipeline.
        Takes three arguments, the first being the ETL orchestration class as a param, the second being the decision of full load vs incremental load, the third being a 
        reference to the task instance to push the results to the airflow metadata database. Lastly ext_type refers to the validation parameter. """

        from datetime import timedelta
        from airflow.operators.python import PythonOperator

        full_load_upsert = PythonOperator(task_id='full_load_upsert',
                                              python_callable=self.full_load_upsert,
                                              do_xcom_push=False,
                                              sla=timedelta(minutes=30))

        return full_load_upsert

    def inc_load_race(self, dag, group_id, default_args):
        """This function calls the extract and load function to kick off the pipeline.
        Takes five arguments, the first first is a reference to the task instance to push the results to the airflow metadata database.
        The next three pertaining to the taskgroup initialization (dag, group_id and default_args), the next being the ETL orchestration class as a param, the last param being the decision of full load vs incremental load."""

        import logging
        from datetime import timedelta
        from airflow.utils.task_group import TaskGroup
        from airflow.operators.python import PythonOperator

        logging.info(
            "-----------------------------------Data extraction, cleaning and conforming-----------------------------------------")

        with TaskGroup(group_id=group_id, default_args=default_args, dag=dag) as incremental_load_race:

            incremental_qualifying = PythonOperator(task_id='incremental_qualifying_load',
                                                    python_callable=self.inc_qualifying,
                                                    do_xcom_push=True,
                                                    sla=timedelta(minutes=10))

            incremental_results = PythonOperator(task_id='incremental_results_load',
                                                 python_callable=self.inc_results,
                                                 do_xcom_push=True,
                                                 sla=timedelta(minutes=10))

        return incremental_load_race

    '''
    def inc_load_telem(self):
        """This function calls the extract and load function to kick off the pipeline.
        Takes five arguments, the first first is a reference to the task instance to push the results to the airflow metadata database.
        The next three pertaining to the taskgroup initialization (dag, group_id and default_args), the next being the ETL orchestration class as a param, the last param being the decision of full load vs incremental load."""
        
        import os
        from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
        from kubernetes.client import models as k8s
        
                
        inc_load_telem = KubernetesPodOperator( 
            namespace="default",
            image="nbroj/airflow-images:airflow-schedulerv1.0.0",
            image_pull_secrets=[k8s.V1LocalObjectReference("")],
            image_pull_policy='Never',
            task_id="inc_load_telemetry",
            command=["python","inc_telem.py"],
            config_file=os.path.expanduser('~')+"/.kube/config",
            do_xcom_push="no",
            on_finish_action="delete_pod",
            in_cluster= False,
            get_logs=True,
            deferrable=True
        )
        return inc_load_telem  
        '''

    def inc_load_pre_transf(self):
        """This function calls the extract and load function to kick off the pipeline.
        Takes five arguments, the first first is a reference to the task instance to push the results to the airflow metadata database.
        The next three pertaining to the taskgroup initialization (dag, group_id and default_args), the next being the ETL orchestration class as a param, 
        the last param being the decision of full load vs incremental load being the validation parameter."""

        from datetime import timedelta
        from airflow.operators.python import PythonOperator

        incremental_load_upsert = PythonOperator(task_id='incremental_load_upsert',
                                                     python_callable=self.inc_load_upsert,
                                                     do_xcom_push=True,
                                                     sla=timedelta(minutes=10))

        return incremental_load_upsert


    def full_trans(self, dag, group_id, default_args):
        """This function returns the task group for the full transform function"""

        from datetime import timedelta
        from airflow.utils.task_group import TaskGroup
        from airflow.providers.postgres.operators.postgres import PostgresOperator

        with TaskGroup(group_id=group_id, default_args=default_args, dag=dag) as transform_full:

            transform_tables_full = PostgresOperator(task_id='transform_tables_full',
                                                     sql='CALL',
                                                     sla=timedelta(minutes=10))

            transform_joining_tables_full = PostgresOperator(task_id='transform_joining_tables_full',
                                                             sql='CALL',
                                                             sla=timedelta(minutes=10))

        return transform_full

    def inc_trans(self, dag, group_id, default_args):
        """This function returns the task group for the transformation phase for the incremental pathway.
            Takes the name of the dag,git  the group_id it belongs to and default args as parameters for the function."""

        from datetime import timedelta
        from airflow.utils.task_group import TaskGroup
        from airflow.providers.postgres.operators.postgres import PostgresOperator

        with TaskGroup(group_id=group_id, default_args=default_args, dag=dag) as transform_inc:

            transform_tables_inc = PostgresOperator(task_id='transform_tables_inc',
                                                    sql='CALL',
                                                    sla=timedelta(minutes=10))

            transform_joining_tables_inc = PostgresOperator(task_id='transform_joining_tables_inc',
                                                            sql='CALL',
                                                            sla=timedelta(minutes=10))
        return transform_inc

    # detecting new data
    def changed_data_handler(self, ti):
        """This class is responsible for determining if there has been any changed data detected between new loaded data
        and that currently in the database tables. Takes two arguments, first being a reference to the postgres client class, and second being a reference to the postgres connection class.
        Works for the incremental pathway"""
        import logging

        extract_dt = ti.xcom_pull(task_ids="inc_ext_load_race", key="incremental_extract_dt")
        logging.info("Creating new postgres connection...")
        pg_conn_uri = self._postgres.get_connection_id(self._pg_conn) # creating the postgres conn uri by calling the function from the client.
        logging.info("Comparing new data to existing data in database...")
        contains_new_data = self._postgres.changed_data_capture(
            pg_conn_uri, extract_dt, self._inc_processed_dir)

        if not contains_new_data == 0: #check if new data was detected inside dataframe 
            return False
        elif contains_new_data == 0:
            return True
        
    def changed_data_detected(self, ti):
        
        """ this function upserts new data into the database after successfully clearing the short circuit operator
        """
        import logging
        
        #pull the extract date from the last loaded body of data 
        extract_dt = ti.xcom_pull(task_ids="", key="")
        logging.info("Creating new postgres connection...")
        pg_conn_uri = self._postgres.get_connection_id(self._pg_conn)
        logging.info("Upserting data into Postgres")

        self._postgres.upsert_db(pg_conn_uri, ti, self._inc_processed_dir, self._inc_full_processed_dir, extract_dt)
