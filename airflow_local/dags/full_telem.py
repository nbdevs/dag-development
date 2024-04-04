import os
from processor import DatabaseETL, WarehouseETL
from director import Director
from connections import PostgresClient, PostgresConnection
from colours import Colours
from decouple import config

# Initializing global variables for duration of data collection
start_date = 2018 # telemetry data only available from 2019 onwards for races
end_date = 2023

# Instantiating classes used within the ETL process
col = Colours()
db_handler = DatabaseETL(col)  # class responsible for database ETL
dw_handler = WarehouseETL(col)  # class responsible for warehouse ETL
# director composes objects which are loaded into database
db_director = Director(start_date, end_date, col, db_handler, dw_handler)

load_type = os.environ["load_type"]

full_extraction_load_telemetry = db_director.telemetry_preprocess(load_type)
