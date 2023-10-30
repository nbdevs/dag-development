from connections import PostgresClient, SnowflakeClient, S3Client, PostgresConnection
from colours import Colours
from processor import DatabaseETL, WarehouseETL

class Director:
    """ This class coordinates the complete ETL pipeline, from API to DB to DW.
    Two functions for each process to upsert into database, and to effective load the data into data stores and then call 
    a stored procedure for the SQL (postgres, snowsql) transformations."""

    import pandas as pd
    
    def __init__(self, startPeriod, endPeriod, col, db_processor, dw_processor) -> pd.DataFrame:
        
        from decouple import config
        
        self._db_builder = db_processor # instantiates the processor class to implement its functions 
        self._dw_builder = dw_processor # instantiates the processor class to implement its functions 
        self._postgres = PostgresClient() # conn string for db dev or dw dev 
        self._snowflake = SnowflakeClient() # snowflake client connection details 
        self._pg_conn = PostgresConnection(1) # postgres connection class 
        self._s3_storage = S3Client() # s3 client connection details
        self._start_date = startPeriod # start period for season of formula 1 data 
        self._end_date = endPeriod # end period for season of formula 1 data 
        self._col = col # A colour formatting class
        self._fl_season_dir = config("season") # env variable for directory to store cache for full load season
        self._fl_results_dir = config("results") # env variable for directory to store cache for full load results
        self._fl_qualifying_dir = config("qualifying") # env variable for directory to store cache for full qualifying
        self._fl_telem_dir = config("telemetry" ) # env variable for directory to store cache for full load telemetry
        self._inc_results = config("inc_results") # env variable for directory to store cache for results
        self._inc_qualifying = config("inc_qualifying") # env variable for directory to store cache for incremental qualifying
        self._inc_qualitelem = config("inc_qualitelem") # env variable for directory to store cache for incremental quali telemetry
        self._inc_racetelem = config("inc_racetelem") # env variable for directory to store cache for incremental race telemetry
        self._full_processed_dir= config("full_processed_dir") # stores the csv files for full load
        self._inc_processed_dir= config("inc_processed_dir") # stores the csv files for incremental load 
    
    def change_data_detected(self, ti, qualifying_table, results_table, race_telem_table, quali_telem_table, extract_dt):
        """ This function follows on from the incremental pathway of the load_db function, after new change data is detected which needs to be reflected in the database.
        If changed data has been detected and the dag run has not been short circuited by the operator then this 
        function serializes and pushes the new data into the database.
        Takes a task instance object as a reference."""
        
        import logging 

        logging.info("Creating new postgres connection...")
        
        # initializing postgres client 
        postgres_client = self._postgres
        postgres_conn_uri = postgres_client.connection_factory(1, self._col,) # calling connection method to get connection string with 1 specified for the database developer privileges
        logging.info("Upserting data into Postgres")
        
        # initializing postgres client 
        postgres_client.upsert_db(postgres_conn_uri, ti, self._inc_processed_dir, self._full_processed_dir, extract_dt)
        
        return
        
    # Creating functions to be used for dag callables

    def retrieve_extract_type(self, ti):
        """This function determines the extract format of the load cycle.
        Takes two arguments, the ETL class that directs the tasks within pipeline, and a reference to a task instance for pushing results
        to the airflow metadata database."""

        from decouple import config
        
        #path to file on linux machine 
        pathway = config("pathway")
        
        # determining if incremental or full load is necessary
        extract_type = self._db_builder.determine_format(pathway)
        # accessing current context of running task instance
        ti.xcom_push(key='extract_format', value=extract_type)
        
        return

    def determine_format(self, ti) -> str:
        """This function determines the branch pathway to follow, determined by the retrieved xcoms 
        value."""

        # accessing current context of running task instance

        extract_type = ti.xcom_pull(key='extract_format', task_ids='retrieve_extract_type')
        
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
                task_group.append('incremental_ext_load_race.{}'.format(tasks[i]))
            
            return task_group

    def full_load_season(self, ti):
        """ This function generates the tabled data for the season level of granularity for F1 races.
        Takes two inputs, one being the db_handler which is a reference to the processor class, and 
        a ti argument for a reference to the task instance of the current running instance"""
        
        import logging
        from decouple import config
        
        # folder for cache for season data
        season_cache = config("season")
        logging.info("Extracting Aggregated Season Data...")
        # outputs aggregated season data as dataframe to be pushed to xcoms
        season_table = self._db_builder.extract_season_grain(season_cache, self._start_date, self._end_date)
        
        # accessing current context of running task instance
        ti.xcom_push(key='season_table', value=season_table)        
        
        return 

    def full_load_qualifying(self, ti):
        """ This function generates the tabled data for the race level of granularity for F1 races.
        Takes two inputs, one being the db_handler which is a reference to the processor class, and 
        a ti argument for a reference to the task instance of the current running instance"""
        
        import logging
        from decouple import config
        
        # cache folder for qualifying folder 
        qualifying_cache = config("qualifying")
        logging.info("Extracting Aggregated Qualifying Data...")
        # outputs aggregated qualifying data as dataframe to be pushed to xcoms
        qualifying_table = self._db_builder.extract_qualifying_grain(qualifying_cache, self._start_date, self._end_date)
        
        # accessing current context of running task instance
        ti.xcom_push(key='qualifying_table', value=qualifying_table)        
        
        return 

    def full_load_results(self, ti):
        """ This function generates the tabled data for the race level of granularity for F1 races.
        Takes two inputs, one being the db_handler which is a reference to the processor class, and 
        a ti argument for a reference to the task instance of the current running instance"""
        
        import logging
        from decouple import config
        
        # cache folder for results cache
        results_cache = config("results")
        logging.info("Extracting Aggregated Results Data...")
        # outputs dataframe containing aggregated results table which will be pushed to xcom
        results_table = self._db_builder.extract_race_grain(results_cache, self._start_date, self._end_date)
        
        # accessing current context of running task instance
        ti.xcom_push(key='results_table', value=results_table) 
   
        return 
        

    def full_load_race_telem(self, ti):
        """ This function generates the tabled data for the telemetry level of granularity for F1 races.
        Takes two inputs, one being the db_handler which is a reference to the processor class, and 
        a ti argument for a reference to the task instance of the current running instance"""
        
        import logging
        from decouple import config
        
        # cache folder for telemetry data
        telem_cache = config("telemetry")
        logging.info("Extracting Aggregated Race Telemetry Data...")
        # outputs dataframe containing all the aggregated race telem data to be outputted to xcoms.
        race_telem_table = self._db_builder.extract_race_telem(telem_cache, self._start_date, self._end_date)

        # accessing current context of running task instance
        ti.xcom_push(key='race_telem_table', value=race_telem_table)        
        
        return 

    def full_load_quali_telem(self, ti):
        """ This function generates the tabled data for the telemetry level of granularity for F1 races.
        Takes two inputs, one being the db_handler which is a reference to the processor class, and 
        a ti argument for a reference to the task instance of the current running instance"""

        import logging
        from decouple import config
        
        # cache folder for telemetry data
        telem_cache = config("telemetry")
        logging.info("Extracting Aggregated Qualifying Telemetry Data...")
        # outputs the dataframe for the qualifying data to be stored in xcoms
        quali_telem_table = self._db_builder.extract_quali_telem(telem_cache, self._start_date, self._end_date)
            
        # accessing current context of running task instance 
        ti.xcom_push(key='quali_telem_table', value=quali_telem_table)        
        
        return 
        
    def full_load_serialize(self, ti):
        """ This function generates the CSV files for the tabled data for all levels of granularity.
        Takes two inputs, one being the db_handler which is a reference to the processor class, and 
        a ti argument for a reference to the task instance of the current running instance"""
        
        import logging
        from decouple import config
        
        # directory which the processed csv files are output to as a result of this function
        processed_dir = config("user_home")
        
        logging.info("Serializing pandas Tables into CSV...")
        
        results_table = ti.xcoms_pull(task_ids='full_results_load', key='results_table')
        qualifying_table = ti.xcoms_pull(task_ids='full_qualifying_load', key='qualifying_table')
        season_table = ti.xcoms_pull(task_ids='full_season_load', key='season_table')
        race_telem_table = ti.xcoms_pull(task_ids='full_race_telem_load', key='race_telem_table')
        quali_telem_table = ti.xcoms_pull(task_ids='full_quali_telem_load', key='quali_telem_table')
        
        # creates the extract date after the function is executed which will be pushed to xcoms.
        extract_dt = self._db_builder.serialize_full(processed_dir, results_table, qualifying_table, season_table, race_telem_table, quali_telem_table)
        
        # pushing value to xcoms
        ti.xcom_push(key='extract_date', value=extract_dt)        
        
        return 

    def full_load_upsert(self, ti):
        """ This function upserts the CSV data into the staging  database tables ready to be transformed into the production 3nf tables. 
        Takes two inputs, one being the db_handler which is a reference to the processor class, and 
        a ti argument for a reference to the task instance of the current running instance"""
        
        import logging
        from decouple import config
        
        # cache folder for incremental and full pathway processed csv files containing table data
        incremental_dir = config("inc_processed_dir")
        full_dir = config("full_processed_dir")
        
        logging.info("Upserting data into Postgres ")
        
        # retrieving the extract_date of the last load from xcom
        extract_dt = ti.xcom_pull(task_ids='full_load_serialization', key = 'extract_date')
        
        #initializing postgres client to generate postgres connection uri
        pg_conn_uri = self._postgres.connection_factory(1, self._col) # calling connection method to get connection string with 1 specified for the database developer privileges
        self._postgres.upsert_db(pg_conn_uri, ti, incremental_dir, full_dir, extract_dt)
        
        return

    def inc_qualifying(self, ti):
        """ This function generates the tabled data for the race level of granularity for F1 races.
        Takes two inputs, one being the db_handler which is a reference to the processor class, and 
        a ti argument for a reference to the task instance of the current running instance"""
        
        import logging
        from decouple import config
        
        # cache folder for qualifying data
        qualifying_dir = config("inc_qualifying")
        
        logging.info("Extracting Aggregated Qualifying Data.")
        
        # outputs the dataframe for the qualifying data to be stored in xcoms
        qualifying_table = self._db_builder.incremental_qualifying(ti)
        
        # accessing current context of running task instance to push to xcoms
        ti.xcom_push(key='qualifying_table', value=qualifying_table)     
        
        return
        
    def inc_results(self, ti):
        """ This function generates the tabled data for the race level of granularity for F1 races.
        Takes two inputs, one being the db_handler which is a reference to the processor class, and 
        a ti argument for a reference to the task instance of the current running instance"""
        
        import logging
        from decouple import config
        
        # cache folder for results data
        results_dir = config("inc_results")
        
        logging.info("Extracting Aggregated Results Data...")
        
        # outputs the dataframe for the race result data to be stored in xcoms
        results_table = self._db_builder.incremental_results(ti)
        
        # accessing current context of running task instance to push to xcoms
        ti.xcom_push(key='results_table', value=results_table)     
            
        return
        
    def inc_quali_telem(self, ti):
        """ This function generates the tabled data for the telemetry level of granularity for F1 races.
        Takes two inputs, one being the db_handler which is a reference to the processor class, and 
        a ti argument for a reference to the task instance of the current running instance"""
        
        import logging
        from decouple import config
        
        # cache folder for telemetry data
        quali_dir = config("inc_qualitelem")
        
        logging.info("Extracting aggregated Quali Telemetry data... ")
        
        # outputs the dataframe for the qualifying telemetry data to be stored in xcoms
        quali_telem_table = self._db_builder.incremental_quali_telem(ti, quali_dir)
        
        # accessing current context of running task instance to push to xcoms
        ti.xcom_push(key='quali_telem_table', value=quali_telem_table)     
            
        return 

    def inc_race_telem(self, ti):
        """ This function generates the tabled data for the telemetry level of granularity for F1 races.
        Takes two inputs, one being the db_handler which is a reference to the processor class, and 
        a ti argument for a reference to the task instance of the current running instance"""
        
        import logging
        from decouple import config
        
        # cache folder for telemetry data
        race_dir = config("inc_racetelem")
        
        logging.info("Extracting aggregated Race Telemetry data... ")
        
        # outputs the dataframe for the race telemetry data to be stored in xcoms
        race_telem_table = self._db_builder.incremental_race_telem(ti, race_dir)
        
        # accessing current context of running task instance to push to xcoms
        ti.xcom_push(key='race_telem_table', value=race_telem_table)     
            
        return 

    def inc_load_serialize(self, ti):
        """ This function generates the CSV files for all levels of granularity which are converted from the previously generated dataframes. 
        Takes two inputs, one being the db_handler which is a reference to the processor class, and 
        a ti argument for a reference to the task instance of the current running instance"""
        
        import logging
        from decouple import config
        
        # directory which the processed csv files are output to as a result of this function
        processed_dir = config("user_home")
        
        logging.info("Serializing pandas dataframes into CSV table format...")
        
        results_table = ti.xcoms_pull(task_ids='incremental_results_load', key='results_table')
        qualifying_table = ti.xcoms_pull(task_ids='incremental_qualifying_load', key='qualifying_table')
        race_telem_table = ti.xcoms_pull(task_ids='incremental_race_telem_load', key='race_telem_table')
        quali_telem_table = ti.xcoms_pull(task_ids='incremental_quali_telem_load', key='quali_telem_table')
        
        # creates the extract date after the function is executed which will be pushed to xcoms.
        extract_dt = self._db_builder.increment_serialize(processed_dir, results_table, qualifying_table, race_telem_table, quali_telem_table)
        
        # pushing value to xcoms
        ti.xcom_push(key='incremental_extract_date', value=extract_dt)        
        
        return 

    def inc_load_upsert(self, ti):
        """ This function upserts the CSV data into the staging  database tables ready to be transformed into the production 3nf tables. 
        Takes two inputs, one being the db_handler which is a reference to the processor class, and 
        a ti argument for a reference to the task instance of the current running instance"""
        
        import logging
        from decouple import config
        
        # cache folder for incremental and full pathway processed csv files containing table data
        incremental_dir = config("inc_processed_dir")
        full_dir = config("full_processed_dir")
        
        logging.info("Upserting data into Postgres ")
        
        # retrieving the extract_date of the last load from xcom
        extract_dt = ti.xcom_pull(task_ids='incremental_load_serialization', key='incremental_extract_date')
        
        #initializing postgres client to generate postgres connection uri
        pg_conn_uri = self._postgres.connection_factory(1, self._col) # calling connection method to get connection string with 1 specified for the database developer privileges
        self._postgres.upsert_db(pg_conn_uri, ti, incremental_dir, full_dir, extract_dt)
        
        return  
            
    # extract and load new data into database

    def full_season_load(self):
        """This function calls the extract and load function to kick off the pipeline.
        Takes three arguments, the first being the ETL orchestration class as a param, the second being the decision of full load vs incremental load, and finally a 
        reference to the task instance to push the results to the airflow metadata database."""

        import logging
        from datetime import timedelta
        from airflow.operators.python import PythonOperator
    
        logging.info("-----------------------------------Data extraction, cleaning and conforming-----------------------------------------")

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
        
    def full_load_telemetry(self, dag, group_id, default_args):
        """This function calls the extract and load function to kick off the pipeline.
        Takes three arguments, the first being the ETL orchestration class as a param, the second being the decision of full load vs incremental load, and finally a 
        reference to the task instance to push the results to the airflow metadata database."""

        from datetime import timedelta
        from airflow.utils.task_group import TaskGroup
        from airflow.operators.python import PythonOperator
        
        with TaskGroup(group_id=group_id, default_args=default_args, dag=dag) as full_load_telemetry:

            
            full_load_race_telem = PythonOperator(task_id='full_race_telem_load',
                                                            python_callable=self.full_load_race_telem,
                                                            do_xcom_push=False,
                                                            sla=timedelta(minutes=100))
            
            full_load_quali_telem = PythonOperator(task_id='full_quali_telem_load',
                                                            python_callable=self.full_load_quali_telem,
                                                            do_xcom_push=False,
                                                            sla=timedelta(minutes=100))

            return full_load_telemetry
        
    def full_load_pre_transformation(self, dag, group_id, default_args):
        """This function calls the extract and load function to kick off the pipeline.
        Takes three arguments, the first being the ETL orchestration class as a param, the second being the decision of full load vs incremental load, the third being a 
        reference to the task instance to push the results to the airflow metadata database. Lastly ext_type refers to the validation parameter. """

        from datetime import timedelta
        from airflow.utils.task_group import TaskGroup
        from airflow.operators.python import PythonOperator
       
        with TaskGroup(group_id=group_id, default_args=default_args, dag=dag) as full_load_pt:
            
            full_load_serialize = PythonOperator(task_id='full_load_serialization',
                                                        python_callable=self.full_load_serialize,
                                                        do_xcom_push=False,
                                                        sla=timedelta(minutes=30))

            full_load_upsert = PythonOperator(task_id='full_load_upsert',
                                                    python_callable=self.full_load_upsert,
                                                    do_xcom_push=False,
                                                    sla=timedelta(minutes=30))
                                    
        return full_load_pt

    def inc_load_race(self, dag, group_id, default_args):
        """This function calls the extract and load function to kick off the pipeline.
        Takes five arguments, the first first is a reference to the task instance to push the results to the airflow metadata database.
        The next three pertaining to the taskgroup initialization (dag, group_id and default_args), the next being the ETL orchestration class as a param, the last param being the decision of full load vs incremental load."""
        
        import logging
        from datetime import timedelta
        from airflow.utils.task_group import TaskGroup
        from airflow.operators.python import PythonOperator
            
        logging.info("-----------------------------------Data extraction, cleaning and conforming-----------------------------------------")
            
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


    def inc_load_telem(self, dag, group_id, default_args):
        """This function calls the extract and load function to kick off the pipeline.
        Takes five arguments, the first first is a reference to the task instance to push the results to the airflow metadata database.
        The next three pertaining to the taskgroup initialization (dag, group_id and default_args), the next being the ETL orchestration class as a param, the last param being the decision of full load vs incremental load."""
        
        from datetime import timedelta
        from airflow.utils.task_group import TaskGroup
        from airflow.operators.python import PythonOperator
        
            
        with TaskGroup(group_id=group_id, default_args=default_args, dag=dag) as incremental_load_telem:

            incremental_quali_telem = PythonOperator(task_id='incremental_quali_telem_load',
                                                            python_callable=self.inc_quali_telem,
                                                            do_xcom_push=True,
                                                            sla=timedelta(minutes=10)
                                                        )                                            

            incremental_race_telem = PythonOperator(task_id='incremental_race_telem_load',
                                                            python_callable=self.inc_race_telem,
                                                            do_xcom_push=True,
                                                            sla=timedelta(minutes=10))
            
        return incremental_load_telem

    def inc_load_pre_transf(self, dag, group_id, default_args):
        """This function calls the extract and load function to kick off the pipeline.
        Takes five arguments, the first first is a reference to the task instance to push the results to the airflow metadata database.
        The next three pertaining to the taskgroup initialization (dag, group_id and default_args), the next being the ETL orchestration class as a param, 
        the last param being the decision of full load vs incremental load being the validation parameter."""
     
        from datetime import timedelta
        from airflow.utils.task_group import TaskGroup
        from airflow.operators.python import PythonOperator
            
        with TaskGroup(group_id=group_id, default_args=default_args, dag=dag) as incremental_load_pt:
            
            incremental_load_serialization = PythonOperator(task_id='incremental_load_serialization',
                                                            python_callable=self.inc_load_serialize,
                                                            do_xcom_push=True,
                                                            sla=timedelta(minutes=10))
            
            incremental_load_upsert = PythonOperator(task_id='incremental_load_upsert',
                                                            python_callable=self.inc_load_upsert,
                                                            do_xcom_push=True,
                                                            sla=timedelta(minutes=10))

        return incremental_load_pt
        
    # detecting new data
    def changed_data_capture(self, ti):
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
        extract_dt = self._db_builder.increment_serialize(qualifying_table, results_table, race_telem_table, quali_telem_table, ti)
        
        # storing the extract_dt in xcoms for later retrieval
        ti.xcom_push(key='incremental_extract_dt', value=extract_dt)
        
        # creating the postgres conn uri by calling the function from the client.
        pg_conn_uri = self._postgres.get_connection_id(self._pg_conn)
        self._postgres.changed_data_capture(pg_conn_uri, extract_dt, results_table, qualifying_table, race_telem_table, quali_telem_table)
        
        return

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


    def changed_data_detect(self, ti):
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
        self._db_builder.changed_data_detected(ti, qualifying_table, results_table, race_telem_table, quali_telem_table, extract_dt)
        
        return
