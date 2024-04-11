from __future__ import annotations
from abc import ABC, abstractmethod

class IConnection(ABC):
    """ Interface which all forms of db connections must implement.
    provisions db connections """
    
    @abstractmethod
    def create_connection(self) -> str:
        """"Default interface method to provision the new db connection string"""
        pass
    
class PostgresConnection(IConnection):
    """ Postgres connection class which provides the connection string for all database users needed for ETL.
        includes a conditional initialization method as well as a connection creation method - both depend on user input at runtime 
        (dag cycles)"""
      
    # protected variable 
    _postgres_uri = ""
    
    def __init__(self, arg):
        
        from decouple import config
        
        if arg == 1:  
            self._postgres_uri = config('POSTGRES_URI_DB')
            
        elif arg == 2:
            self._postgres.uri = config('POSTGRES_URI_DW')
        
    def create_connection(self) -> str: 
        """Generate postgresql connection string for db connection"""
        
        # take input in main program which validates input and produces conn string 
        # for one of two database users
        
        postgres_uri = self._postgres_uri
        return postgres_uri
    
class SnowflakeConnection(IConnection):
    """ SnowflakeConnection class which provides a connection URI for all users of the snowflake data warehouse
        will provision read/write access to the dw dev for airflow processes."""
        
    # protected variable 
    _snowflake_uri = ""
    
    def __init__(self):
        
        from decouple import config
        
        self._snowflake_uri = config('SNOWFLAKE_URI')
 
    def create_connection(self) -> str: 
        """ Generate snowflake connection string for db connection"""
        
        # take input in main program which validates input and produces conn string from .env file 
        snowflake_uri = self._snowflake_uri 
    
        return snowflake_uri
    
    def upsert_wh(self, conn, ti):
        pass
     
class S3Connection(IConnection):
    """ S3Connection class which provisions connections to s3 buckets for cloud storage. 
        A bucket exists for each of:
        DB ETL, 
        DAG logs 
        ,and DW ETL."""
        
    # protected variables 
    _s3_key = ""
    _s3_secret = ""
    _s3_region = ""
    _s3_bucket = ""
    
    def __init__(self):
        
        from decouple import config

        self.aws_key = config('AWS_KEY_ID')
        self.aws_secret_key = config('AWS_SECRET_KEY')
        self.aws_region = config('AWS_REGION')
        self.aws_bucket = config('AWS_BUCKET_NAME')
        
    def create_connection(self) -> str: 
        """ Generate s3 connection variables for bucket access """
        
        # generate aws access which grants access to s3 bucket for storage
   
        s3_key = self._s3_key  #s3 aws key 
        s3_secret = self._s3_secret #s3 secret key
        s3_region = self._s3_region #s3 region
        s3_bucket = self._s3_bucket #s3 bucket

        return s3_key, s3_secret, s3_region, s3_bucket
    
class AbstractClient(ABC):
    
    @abstractmethod
    def connection_factory(self):
        """Factory method which may be overridden by subclasses in order to provide their own implementations"""
        pass
 
    def get_connection_id(self, database_conn):
        """ Base method for generating db connection which all subclasses must implement"""
        conn = database_conn.create_connection()
    
class PostgresClient(AbstractClient):

    """ The client class for retrieving connections in order to access postgres database"""
    
    def connection_factory(self, arg, col) -> IConnection:
        """ Provision connections to either the db dev or dw dev"""
        import logging
        
        try:
            if arg == 1: # flow of control for the database developer in ETL 
                postgres = PostgresConnection(arg)
                # call function to create connection
                postgres_conn = self.get_connection_id(postgres)
            elif arg == 2: # flow of control for the data warehouse developer in etl 
                postgres = PostgresConnection(arg)
                # call function to create connection
                postgres_conn = self.get_connection_id(postgres)
        except ValueError:
                logging.error(col.boldFont + col.redFont +
                  "[ERROR] " + col.endFont + "Integer out of range.")
            
        return postgres_conn
    
    def upsert_db(self, conn, ti, inc_processed_dir, full_processed_dir, extract_dt):
        """ This function is responsible for extracting CSVs from local file system into postgres db. 
            Variables used in this function are the connection URI  and a reference to ti module for the
            decision variable that relates to either a 'full' or 'incremental load'"""
        
        import os
        import logging
        from psycopg2 import OperationalError, ProgrammingError
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        # instantiating postgres client details to pass to pg hook 
        try:
            pg_hook = PostgresHook(postgres_conn_id=conn)
            pg_connection = pg_hook.get_conn()
        except OperationalError:
            logging.error("Database URI is incorrect.")
            
        #determining the load type for the database from the branch operator of airflow 
        check_load_type = ti.xcom_pull(task_ids='determine_extract_format', key='extract_format')
          
        if check_load_type == "Incremental":
            dest_dir = inc_processed_dir
        else: 
            try:
                assert check_load_type == "Full"
                dest_dir = full_processed_dir
            except AssertionError:
                logging.error("'check_load_type' variable in airflow task_id empty.")
                logging.info("Ensure this variable has been set.")
       
        # for each file in the destination directory 
        for filename in os.listdir(dest_dir):
            file = os.path.join(dest_dir, filename) # join destination directory and filename to same path
            if os.path.isfile(file): # check the full path to the file exists
        
                # strip extract date and "csv" label from filename
                table_name = file.strip(f'"{extract_dt}.csv"')
                # query to copy to table in database whilst removing csv header delimiter
                query = 'COPY {} FROM STDIN WITH CSV HEADER DELIMITER ',''.format(table_name)
                try:
                    pg_hook.copy_expert(query, file)
                except ProgrammingError:
                    logging.error("Error writing data to table.")
                finally: 
                    pg_connection.close()
    
        return
        
    def changed_data_capture(self, conn, extract_dt, inc_processed_dir):
        """ Check the database tables for new rows to insert these ones only. 
            This is a function aimed for incremental updates of race data"""
           
        from datetime import datetime
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        import pandas as pd
    
        pg_hook = PostgresHook(conn)
        pg_connection = pg_hook.get_conn()
        pg_cursor = pg_connection.cursor()
        
        #export table to csv then convert to dataframes 
        #compare table data to new dataframe with diff/merge/compare function
        #extract the new data only from the new dataframe if there is any 
        #call short circuit opertaor if no new data, but if new THEN
        
        #if new data then return 1 AND
        #  upsert these into database tables with upsert_db function
        
        #else if no new data return 0 AND
        # short circuit dag cycle
        
        pass
      
class SnowflakeClient(AbstractClient):
    """The SnowflakeClient class represents the user who requires the connection and fetches it from the connection class. """
    def connection_factory(self) -> IConnection:
        """ Provision connections to the dw dev and the data analyst by instantiating connection class and invoking its method. 
        then retrieves the connection URI from the connection class and passes it to the client."""
        
        snowflake = SnowflakeConnection(self)
        
        #calling connection_factory method to create new conn string
        snowflake_conn = self.get_connection_id(snowflake)
        
        return snowflake_conn
    
class S3Client(AbstractClient):
    """This class represents the client who wishes to utilise an AWS S3 connection"""
    
    def get_connection_id(self, database_conn):
        """Overriding default implementation for unpacking of tuple """
        
        s3_key, s3_secret, s3_region, s3_bucket  = self.get_connection_id(database_conn)
        
    def connection_factory(self) -> IConnection:
        """ Provision connections to the dw dev only by instantiating s3 class and invoking its method.
            Then retrieves the s3 aws key, secret key, region name and the name of the s3 bucket to pass to the client."""
        
        s3 = S3Connection(self)
        
        #calling connection_factory method to pass conn details to dw dev
        s3_key, s3_secret, s3_region, s3_bucket  = self.get_connection_id(s3)

        return s3_key, s3_secret, s3_region, s3_bucket
          

if __name__ == "__main__":
    postgres = PostgresClient()
    from colours import Colours
    col = Colours()
    pg_conn_uri = postgres.connection_factory(1, col)
    