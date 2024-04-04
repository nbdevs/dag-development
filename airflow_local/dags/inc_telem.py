from processor import DatabaseETL, WarehouseETL
from director import Director
from connections import PostgresClient, PostgresConnection
from colours import Colours
from decouple import config

# Initializing global variables for duration of data collection
start_date = 2018 # telemetry data only available from 2019 onwards for races
end_date = 2023

quali_telem_dir = config("inc_qualitelem")
race_telem_dir = config("inc_racetelem")

# Instantiating classes used within the ETL process
col = Colours()
db_handler = DatabaseETL(col)  # class responsible for database ETL
dw_handler = WarehouseETL(col)  # class responsible for warehouse ETL
# director composes objects which are loaded into database
db_director = Director(start_date, end_date, col, db_handler, dw_handler)

inc_q_t = db_director.inc_quali_telem(start_date, end_date, quali_telem_dir) 
inc_r_t = db_director.inc_race_telem(race_telem_dir) 