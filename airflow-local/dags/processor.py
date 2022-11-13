""" Responsible for data extraction, cleaning and loading, as well as data transformations via sql procedure calls."""
from __future__ import annotations
from abc import ABC, abstractmethod
from connections import PostgresClient, SnowflakeClient, S3Client
from colours import Colours
from decouple import config
import datetime
import pandas as pd
# --------------------------------------------------------------------------------------------------

class Processor(ABC):
    """ Processor interface which creating different forms of data tables representing formula 1 data for the 3 different grains 
    in the system, aggregates them accordingly and passes the result to airflow to store in x-comms.
    Base class with empty functions for overriding."""

    @abstractmethod
    def determine_format(self, ti):
        pass

    @abstractmethod
    def extract_season_grain(self, cache_dir, start_date, end_date) -> pd.DataFrame:
        pass
       
    @abstractmethod
    def extract_qualifying_grain(self, cache_dir, start_date, end_date) -> pd.DataFrame:
        pass
    
    @abstractmethod
    def extract_race_grain(self, cache_dir, start_date, end_date) -> pd.DataFrame:
        pass

    @abstractmethod
    def incremental_qualifying(self, cache_dir) -> pd.DataFrame:
        pass
    
    @abstractmethod
    def incremental_results(self, cache_dir) -> pd.DataFrame:
        pass
    
    @abstractmethod
    def serialize_full(self, race_table, driver_table, season_table, race_telem_table, quali_telem_table)-> datetime:
        pass
    
    @abstractmethod
    def increment_serialize(self, qualifying_table, results_table, race_telem_table, quali_telem_table, ti)-> datetime:
        pass
    
    @abstractmethod
    def extract_quali_telem(self, cache_dir, start_date, end_date) -> pd.DataFrame:
        pass
    
    @abstractmethod
    def extract_race_telem(self, cache_dir, start_date, end_date) -> pd.DataFrame:
        pass
    
    @abstractmethod
    def postgres_transformations(self) -> None:
    
    # these tables will be creating by the T part in SQL  and called in create_table_links

    #  create_driver_qualifying(self) -> pd.DataFrame:
       
    #  create_race_team(self) -> pd.DataFrame:
    
    # create_table_links(self) -> pd.DataFrame:
        pass
       
    @abstractmethod
    def create_table_links(self) -> None:
        pass

    # view creation -- stored procedure to create the 24 or so views for driver champ standing 
    # then can copy contents of this table into LIKE table in the 
    # data warehouse rather than attempting to recreate from dimensional model
     
    @abstractmethod
    def create_championship_views(self, postgres_uri) -> None:
        pass


class DatabaseETL(Processor):
    """Follows the implementation details provided in the interface in order to build the dataframe in the specified manner needed for the respective data grain.
    Class for pre-processing/aggregation of the formula one data from API."""

    def __init__(self, col) -> pd.DataFrame:
        """Each new instance contains an colours class for formatting."""
        self._col = col
   
    def determine_format(self, ti) -> str:
        """Function which determines if incremental or full load is necessary."""
 
        import logging
        from airflow.exceptions import XComNotFound
        
        try:
            ti.xcom_pull(key='extract_format',task_id='_determine_format')
            logging.info("Incremental load chosen as extract format")
            load_type = "extract_incremental"
            ti.xcom_push(key='extract_format', value=load_type)
            return load_type
            
        except XComNotFound:
            logging.error("XComms variable has not been created...")
            logging.info("Full load chosen as extract format...")
            load_type = "extract_full"
            ti.xcom_push(key='extract_format', value=load_type)
            return load_type
        
    def postgres_transformations(self):
        pass
        
    def extract_season_grain(self, cache_dir, start_date, end_date) -> pd.DataFrame:
        """ Extracts the season grain data from the api and aggregates them together ready for loading into the database.
            takes a target cache directory as the input for first variable,
            start date and end date for extraction of f1 data as the next two. """
       
        import os 
        import logging
        import fastf1 as f1
        import pandas as pd
        import numpy as np 
      
        logging.info("------------------------------Extracting Aggregated Season Data--------------------------------------")

        f1.Cache.enable_cache(cache_dir) # enable cache for faster data retrieval

        data_holder = []
        #outer for loop for seasons
        for s in range(start_date, end_date):
            size = f1.get_event_schedule(s)
            size["season_year"] = s
            data_holder.append(size)
        
        #CREATE data frame for season data
        for d in data_holder:
            
            season = pd.concat(data_holder)

            # drop session 1,2,3, and dates, f1api support, officialeventname from season data

            columns = ["OfficialEventName", "Session1", "Session1Date", "Session2", "Session2Date", "Session3", "Session3Date", "F1ApiSupport", "EventFormat", "Session4", "Session5", "EventDate"]
            for c in columns:
                season.drop(c, axis=1, inplace=True)

            #rename columns
            #columns = {RoundNumber:race_round, Country:country, Location:city, EventName:race_name, Session4Date:quali_date, Session5Date:race_date}
            #season.rename(columns, inplace=True, axis='columns')
            season.columns = ["race_round", "country", "city", "race_name", "quali_date", "races_date", "season_year"]
            season.index += 1
            season.index.name = "race_id"
            season.head()

            #splitting the date and time apart
            season["race_date"] = pd.to_datetime(season['races_date']).dt.date
            season["race_time"] = pd.to_datetime(season["races_date"]).dt.time

            season.drop("races_date", axis=1, inplace=True)
            season_table  = season
            
            return season_table
        
    def extract_qualifying_grain(self, cache_dir, start_date, end_date) -> pd.DataFrame:
        """ Extracts the qualifying/driver aggregated data from the api and stores in the dataframe.
            Takes a target cache directory as the first variable
            start date and end date date for data extraction for the rest."""
        
        import os 
        import logging 
        import fastf1 as f1 
        import pandas as pd
        import numpy as np
        
        logging.info("------------------------------Extracting Aggregated Qualifying Data--------------------------------------")
                
        f1.Cache.enable_cache(cache_dir) #enable cache for faster data retrieval
        #outer for loop for seasons
        for s in range(start_date, end_date):
            size = f1.get_event_schedule(s)
            
            # get the number of races in the season by getting all race rounds greater than 0
            race_no = size.query("RoundNumber > 0").shape[0]
            print(race_no)
            race_name_list = [] # empty list for storing races 
            
            #get all the names of the races for this season
            for i in range(1, race_no):

                jam = f1.get_event(s, i)
                race_name = jam.loc["EventName"] # access by column to get value of race
                print(race_name)
                race_name_list.append(race_name)

                ff1 = f1.get_session(s, i, 'Q')
                #load all driver information for session
                ff1.load()
                drivers = ff1.drivers
                listd = []
                #loop through every driver in the race 
                for d in drivers:

                    name = ff1.get_driver(d)
                    newname = name.to_frame().T #invert columns and values 

                    columns = ["BroadcastName", "Time", "FullName", "Status", "Points", "GridPosition"]
                    #drop irrelevant columns 
                    for c in columns:
                        newname.drop(c, axis=1, inplace=True)
                    
                    #provide desired column names 
                    newname.columns = ["car_number", "driver_id", "team_name", "team_colour", "forename", "surname", "quali_pos", "best_q1", "best_q2", "best_q3"]
                    #provide new index 
                    newname.index.name = "driver_id"

                    newname["season_year"] = s
                    newname["race_name"] = race_name
                    newname
                    #convert time deltas to strings and reformat 
                    col=["best_q1","best_q2", "best_q3"]
                    for c in col:
                        newname[c] = newname[c].astype(str).map(lambda x: x[10:])

                    #replace all empty values with NaN
                    newname.replace(r'^\s*$', np.nan, regex=True, inplace=True)
                    listd.append(newname)

                drive = pd.concat(listd) #append to end of the dataframe
                driver_table = drive
                 
        return driver_table
        
    def extract_race_grain(self, cache_dir, start_date, end_date) -> pd.DataFrame:
        """ Extracts race/result aggregated data and stores in pandas dataframe"""
        
        import fastf1 as f1 
        import pandas as pd
        import numpy as np
        import os 

        f1.Cache.enable_cache(cache_dir) # enable cache for faster data retrieval 

        #outer for loop for seasons
        for s in range(start_date, end_date):
            size = f1.get_event_schedule(s)
            
            # get the number of races in the season by getting all race rounds greater than 0
            race_no = size.query("RoundNumber > 0").shape[0]
            print(race_no)
            race_name_list = [] # empty list for storing races 
            
            #get all the names of the races for this season
            for i in range(1, race_no):

                jam = f1.get_event(s, i)
                race_name = jam.loc["EventName"] # access by column to get value of race
                print(race_name)
                race_name_list.append(race_name)

                ff1 = f1.get_session(s, i, 'R')
                #load all driver information for session
                ff1.load()
                drivers = ff1.drivers
                listd = []
                
                #loop through every driver in the race 
                for d in drivers:

                    name = ff1.get_driver(d)
                    newname = name.to_frame().T #invert columns and values 

                    columns = ["BroadcastName", "FullName", "Q1", "Q2", "Q3"]
                    #drop irrelevant columns 
                    for c in columns:
                        newname.drop(c, axis=1, inplace=True)
            
                    #provide desired column names 
                    newname.columns = ["car_number", "driver_id", "team_name", "team_colour", "forename", "surname", "finish_pos", "start_pos", "Time", "driver_status_update", "race_points"]
                    #provide new index 
                    newname.index.name = "driver_id"
                    
                    newname["pos_change"] =  newname["start_pos"] - newname["finish_pos"]
                    newname["season_year"] = s
                    newname["race_name"] = race_name
                    newname
                    #convert time deltas to strings and reformat 
                    col=["Time"]
                    
                    for c in col:
                        newname[c] = newname[c].astype(str).map(lambda x: x[10:])

                    #replace all empty values with NaN
                    newname.replace(r'^\s*$', np.nan, regex=True, inplace=True)
                    listd.append(newname)

                season_table = pd.concat(listd)
                
        return season_table
    
    def serialize_full(self, race_table, driver_table, season_table, race_telem_table, quali_telem_table) -> datetime:
        """ This method is responsible for serializing the files into csv format before they are uploaded in raw form to an s3 bucket for persistence."""
        import os 
        import logging
        from datetime import datetime 
        import pandas as pd 
        
        logging.info("------------------------------Serializing DataFrames to CSV--------------------------------------")
        
        extract_dt = datetime.today().strftime("%Y-%m-%d")

        dataframes = [race_table, driver_table, season_table, race_telem_table, quali_telem_table]
        user_home = config("user_home")
        
        i = 0 # counter variable 
        
        #append date to the end of the files, and output to the correct directory
        for d in dataframes:
            
            i+=1 # increment counter 
            
            if i == 1:
                d.to_csv('Users/{}/Documents/Cache/FullProcessed/Results{}.csv'.format(user_home, extract_dt), index=False, header=True)
                
            elif i == 2:
                d.to_csv('Users/{}/Documents/Cache/FullProcessed/Qualifying{}.csv'.format(user_home, extract_dt), index=False, header=True)
                
            elif i == 3:
                d.to_csv('/Users/{}/Documents/Cache/FullProcessed/Season{}.csv'.format(user_home, extract_dt), index=False, header=True)
            
            elif i == 4:
                d.to_csv('/Users/{}/Documents/Cache/FullProcessed/RaceTelem{}.csv'.format(user_home, extract_dt), index=False, header=True)
                
            elif i == 5:
                d.to_csv('/Users/{}/Documents/Cache/FullProcessed/QualifyingTelem{}.csv'.format(user_home, extract_dt), index=False, header=True)
                
        return extract_dt # return the date to push to xcoms for s3 hook to identify the file by the date for uploading into postgres 
       
    def incremental_qualifying(self):
        """This function handles the incremental extraction of qualifying data into the database"""
        
        import logging
        import os
        from datetime import datetime 
        
        extract_dt = datetime.today().strftime("%Y-%m-%d")
        cache_dir = config("incremental_qualifying")
        cache_dp = cache_dir.format(extract_dt)
        
        pass
    
    def incremental_results(self):
        """This function handles the incremental loading of race result data into the database"""
        
        import logging
        import os
        from datetime import datetime 
        
        extract_dt = datetime.today().strftime("%Y-%m-%d")
        cache_dir = config("incremental_results")
        cache_dp = cache_dir.format(extract_dt)
        
        pass
    
    def increment_serialize(self, qualifying_table, results_table, race_telem_table, quali_telem_table, ti):
        
        import logging
        import os
        from datetime import datetime 
        
        logging.info("------------------------------Serializing DataFrames to CSV--------------------------------------")
        
        extract_dt = datetime.today().strftime("%Y-%m-%d")

        dataframes = [qualifying_table, results_table, race_telem_table, quali_telem_table]
        user_home = config("user_home")
        
        i = 0 # counter variable 
        
        #append date to the end of the files, and output to the correct directory
        for d in dataframes:
            
            i+=1 # increment counter 
            
            if i == 1:
                d.to_csv('Users/{}/Documents/Cache/IncrementalProcessed/Qualifying{}.csv'.format(user_home, extract_dt), index=False, header=True)
                
            elif i == 2:
                d.to_csv('Users/{}/Documents/Cache/IncrementalProcessed/Results{}.csv'.format(user_home, extract_dt), index=False, header=True)
                
            elif i == 3:
                d.to_csv('/Users/{}/Documents/Cache/IncrementalProcessed/RaceTelem{}.csv'.format(user_home, extract_dt), index=False, header=True)
                
            elif i == 4:
                d.to_csv('/Users/{}/Documents/Cache/IncrementalProcessed/QualifyingTelem{}.csv'.format(user_home, extract_dt), index=False, header=True)
                
        ti.xcom_push(key='incremental_extract_dt', value=extract_dt) # return the date to push to xcoms for s3 hook to identify the file by the date for uploading into postgres 
        
        return
     
    def extract_quali_telem(self, cache_dir, start_date, end_date) -> pd.DataFrame:
        """ Full load of telemetry data for qualifying for each event of the race calendar.
            Takes the cache directory, start and end date for data load as parameters, and returns a dataframe."""
          
        import pandas as pd
        import fastf1 as f1
        import numpy as np

        f1.Cache.enable_cache(cache_dir) # enabling cache for faster data retrieval 

        #outer for loop for seasons
        for s in range(start_date, end_date):
            size = f1.get_event_schedule(s)
            # get the number of races in the season by getting all race rounds greater than 0
            race_no = size.query("RoundNumber > 0").shape[0]
            race_name_list = [] # empty list for storing races 
            
            #get all the names of the races for this season
            for i in range(1, race_no):

                event = size.get_event_by_round(i)
                race_name = event.loc["EventName"] # access by column to get value of race
                race_name_list.append(race_name) 
                session = f1.get_session(s, i, identifier='Q', force_ergast=False)
                
                #load all driver information for session
                session.load(telemetry=True, laps=True, weather=True)
                #load weather data
                weather_data = session.laps.get_weather_data()
                
                # selecting columns to eb droipped from weather df
                col = ["Time", "AirTemp", "Pressure", "WindDirection"]
                for c in col:
                    weather_data.drop(c, axis=1, inplace=True)
                    weather_data = weather_data.reset_index(drop=True)

                drivers = session.drivers #list of drivers in the session
                listd = []
                series = []
                #loop through every driver in the race 
                for d in drivers:
            
                    driver_t = session.laps.pick_driver(d) # load all race telemetry session for driver
                    driver_telem = driver_t.reset_index(drop=True)
                    listd.append(driver_telem) # append information to list
                    drive = pd.concat(listd) # concat to pandas series 
                    drive["season_year"] = s # add the season the telemetry pertains to
                    drive["race_name"] = race_name # add the race the telemetry pertains to
                    drive["pole_lap"] = session.laps.pick_fastest()
                    
                    #telemetry data for drivers
                    try:
                        telemetry = session.laps.pick_driver(d).get_telemetry()
                        columns = ["Time", "DriverAhead", "SessionTime", "Date", "DRS", "Source", "Distance", "RelativeDistance", "Status", "X", "Y", "Z", "Brake"]
                        for c in columns: #dropping irrelevant columns
                            telemetry.drop(c, axis=1, inplace=True) 
                        driver_telem = telemetry.reset_index(drop=True) #dropping index
                        dt_quali = pd.DataFrame.from_dict(driver_telem) # creating dataframe from dict object 
                        series.append(dt_quali) #appending to list
                        driver_t = pd.concat(series) #concatenating to dataframe

                        #append weather data, and telemetry data to existing dataframe of lap data
                        telem = pd.concat([drive, weather_data, driver_t], ignore_index=True, sort=False)
                    except ValueError:
                        print("No data available for car number: {}".format(d))
                        continue
        
        column = ["SpeedST", "IsPersonalBest", "PitOutTime", "PitInTime", "TrackStatus","LapStartTime", "LapStartDate", "Sector1SessionTime", "FreshTyre", "Time", "Sector2SessionTime", "Sector3SessionTime", "SpeedI1", "SpeedI2", "WindSpeed", "SpeedFL", "DistanceToDriverAhead"]
        #drop irrelevant columns 
        for c in column:
            telem.drop(c, axis=1, inplace=True)
        
        #convert time deltas to strings and reformat 
        col=["LapTime", "Sector1Time", "Sector2Time", "Sector3Time", "pole_lap"]
        for c in col:
            telem[c] = telem[c].astype(str).map(lambda x: x[10:])

        #replace all empty values with NaN
        telem.replace(r'^\s*$', np.nan, regex=True, inplace=True)

        #provide desired column names 
        telem.columns = ["car_no", "lap_time", "lap_no", "s1_time", "s2_time", "s3_time", "compound", "tyre_life", "race_stint", "team_name", "driver_identifier", "IsAccurate", "season_year", "race_name", "pole_lap", "humidity", "occur_of_rain_quali", "track_temp", "revs_per_min", "car_speed", "gear_no", "throttle_pressure"]

        return telem

    
    def extract_race_telem(self, cache_dir, start_date, end_date) -> pd.DataFrame:
        """Full load of telemetry data for qualifying for each event of the race calendar.
            Takes the cache directory, start and end date for data load as parameters, and returns a dataframe."""
        
        import pandas as pd
        import fastf1 as f1
        import numpy as np

        f1.Cache.enable_cache(cache_dir) #enabling cache for faster data retrieval 

        #outer for loop for seasons
        for s in range(start_date, end_date):
            size = f1.get_event_schedule(s)
            # get the number of races in the season by getting all race rounds greater than 0
            race_no = size.query("RoundNumber > 0").shape[0]
            race_name_list = [] # empty list for storing races 
            
            #get all the names of the races for this season
            for i in range(1, race_no):

                event = size.get_event_by_round(i)
                race_name = event.loc["EventName"] # access by column to get value of race
                race_name_list.append(race_name) 
                session = f1.get_session(s, i, 'R')
                
                #load all driver information for session
                session.load(telemetry=True, laps=True, weather=True)
                
                #testing if telemtry data has been loaded
                try:
                    session.laps.pick_driver("HAM").get_telemetry() 
                except Exception as exc:
                    print(f'"Telemetry data not available for {race_name}"')
                    continue # move to the next race 

                #load weather data
                weather_data = session.laps.get_weather_data()
                    
                # selecting columns to eb droipped from weather df
                col = ["Time", "AirTemp", "Pressure", "WindDirection"]
                for c in col:
                    weather_data.drop(c, axis=1, inplace=True)
                    weather_data = weather_data.reset_index(drop=True)

                drivers = session.drivers
                listd = []
                series = []
                #loop through every driver in the race 
                for d in drivers:
                    driver_t = session.laps.pick_driver(d) # load all race telemetry session for driver
                    driver_telem = driver_t.reset_index(drop=True)
                    listd.append(driver_telem) # append information to list
                    drive = pd.concat(listd) # concat to pandas series
                    drive["season_year"] = s # add the season the telemetry pertains to
                    drive["race_name"] = race_name # add the race the telemetry pertains to
                    drive["fastest_lap"] = session.laps.pick_driver(d).pick_fastest()
                    
                    #telemetry data for drivers
                    try:
                        telemetry = session.laps.pick_driver(d).get_telemetry()
                        columns = ["Time", "DriverAhead", "SessionTime", "Date", "DRS", "Source", "Distance", "RelativeDistance", "Status", "X", "Y", "Z", "Brake"]
                        for c in columns: #dropping irrelevant columns
                            telemetry.drop(c, axis=1, inplace=True) 
                        driver_telem = telemetry.reset_index(drop=True) #dropping index
                        dt_quali = pd.DataFrame.from_dict(driver_telem) # creating dataframe from dict object 
                        series.append(dt_quali) #appending to list
                        driver_t = pd.concat(series) #concatenating to dataframe

                        #append weather data, and telemetry data to existing dataframe of lap data
                        telem = pd.concat([drive, weather_data, driver_t], ignore_index=True, sort=False)
                    except ValueError:
                        print("No data available for car number - {}".format(d))
                        continue
                    
        column = ["TrackStatus","LapStartTime", "LapStartDate", "Sector1SessionTime", "FreshTyre", "Time", "Sector2SessionTime", "Sector3SessionTime", "SpeedI1", "SpeedI2", "WindSpeed", "SpeedFL", "SpeedST"]
        #drop irrelevant columns 
        for c in column:
            telem.drop(c, axis=1, inplace=True)
        
        #convert time deltas to strings and reformat 
        col=["LapTime", "Sector1Time", "Sector2Time", "Sector3Time", "fastest_lap"]
        for c in col:
            telem[c] = telem[c].astype(str).map(lambda x: x[10:])

        #replace all empty values with NaN
        telem.replace(r'^\s*$', np.nan, regex=True, inplace=True)

        #provide desired column names 
        telem.columns =["car_no", "lap_time", "lap_no", "time_out", "time_in", "s1_time", "s2_time", "s3_time", "IsPersonalBest", "compound", "tyre_life", "race_stint", "team_name", "driver_identifier", "IsAccurate", "season_year", "race_name", "fastest_lap", "pit_duration", "humidity", "occur_of_rain_race", "track_temp",  "revs_per_min", "car_speed", "gear_no", "throttle_pressure"]

        telem

    def create_table_links(self, postgres_uri):
        """ This function will create all the joining tables as an intermediary transformation step after 
        the initial tables have been constructed."""
        pass

    def create_championship_views(self, postgres_uri):
        """ This function will create the two views of the drivers championship and the constructors championship dynamically
        through stored procedures which append the race date to the end of the view name."""
        pass

class WarehouseETL(Processor):
    """This class handles the ETL process for the Data Warehouse in the specified manner needed for the respective data grain.
    Class for ETL from AWS S3 files into s3 stage in Snowflake."""
    
    def __init__(self, col) -> pd.DataFrame:
        """Each new instance contains a colours class used for formatting."""
        self._col = col

    def determine_format(self, ti):
        pass
    
    def extract_season_grain(self, cache_dir, start_date, end_date) -> pd.DataFrame:
        pass
       
    def extract_qualifying_grain(self, cache_dir, start_date, end_date) -> pd.DataFrame:
        pass
    
    def extract_race_grain(self, cache_dir, start_date, end_date) -> pd.DataFrame:
        pass

    def incremental_qualifying(self, cache_dir) -> pd.DataFrame:
        pass
    
    def incremental_results(self, cache_dir) -> pd.DataFrame:
        pass
    
    def serialize_full(self, race_table, driver_table, season_table, race_telem_table, quali_telem_table)-> datetime:
        pass
    
    def increment_serialize(self, qualifying_table, results_table, race_telem_table, quali_telem_table, ti)-> datetime:
        pass
    
    def postgres_transformations(self) -> None:
        pass
       
    def create_table_links(self) -> None:
        pass
     
    def create_championship_views(self, postgres_uri) -> None:
        pass
    
    def extract_quali_telem(self, cache_dir) -> None:
        pass
    
    def extract_race_telem(self, cache_dir) -> None:
        pass

class Director:
    """ This class coordinates the complete ETL pipeline, from API to DW.
    Two functions for each process to upsert into database, and to effective load the data into data stores and then call 
    a stored procedure for the SQL (postgres, snowsql) transformations."""

    def __init__(self, startPeriod, endPeriod, col, db_processor, dw_processor) -> pd.DataFrame:
        self._db_builder = db_processor # instantiates the processor class to implement its functions 
        self._dw_builder = dw_processor # instantiates the processor class to implement its functions 
        self._postgres = PostgresClient() # conn string for db dev or dw dev 
        self._snowflake = SnowflakeClient() # snowflake client connection details 
        self._s3_storage = S3Client() # s3 client connection details
        self._startPeriod = startPeriod # start period for season of formula 1 data 
        self._endPeriod = endPeriod # end period for season of formula 1 data 
        self._col = col # A colour formatting class
        self._fl_season_dir = config("season") # env variable for directory to store cache for full load season
        self._fl_results_dir = config("results") # env variable for directory to store cache for full load results
        self._fl_qualifying_dir = config("qualifying") # env variable for directory to store cache for full qualifying
        self._fl_telem_dir = config("telemetry" )# env variable for directory to store cache for full load telemetry
        self._inc_results = config("inc_results") # env variable for directory to store cache for results
        self._inc_qualifying = config("inc_qualifying") # env variable for directory to store cache for incremental qualifying
        self._inc_telem = config("inc_telemetry") # env variable for directory to store cache for incremental telemetry
        self._full_processed_dir= config("full_processed_dir") # stores the csv files for full load
        self._inc_processed_dir= config("inc_processed_dir") # stores the csv files for incremental load 

def changed_data_detected(self, ti):
    """If changed data has been detected and the dag run has not been short circuited by the operator then this 
    function serialises and pushes the new data into the database.
    Takes a task instance object as a reference."""
    
    import logging
    
    logging.info("Loading dataframes stored within xcomms...")
    results_table = ti.xcom_pull(task_ids='incremental_extract_load', key='results_table')
    qualifying_table = ti.xcom_pull(task_ids='incremental_extract_load', key='qualifying_table')
    race_telem_table = ti.xcom_pull(task_ids='incremental_extract_load', key='race_telem_table')
    quali_telem_table = ti.xcom_pull(task_ids='incremental_extract_load', key='quali_telem_table')
    
    logging.info("Serializing pandas Tables into CSV...")
    extract_dt = self._db_builder.increment_serialize(results_table, qualifying_table, race_telem_table, quali_telem_table)
    logging.info("Creating new postgres connection...")
    #initialising postgres client 
    conn = self._postgres
    postgres_conn = conn.connection_factory(self._col, 1) #calling connection method to get connection string with 1 specified for the database developer privileges
    logging.info("Upserting new data into postgres database...")
    self.upsert_db(postgres_conn, ti, self._inc_processed_dir, self._full_processed_dir, extract_dt)

    return 

def load_db(self, decision, ti) -> None:
    """ Helper function which coordinates the extract and cleaning processes involved in a full and incremental load.
        Which process is implemented depends on the decision variable of either 1 or 2."""

    # Calling methods to create pandas dataframes for full load - season wide
    if decision == 1:

        print("-----------------------------------Data extraction, cleaning and conforming-----------------------------------------")
        print("Extracting Aggregated Season Data...")
        season_table = self._db_builder.extract_season_grain(self._fl_season_dir, self._start_date, self._end_date)
        print("Extracting Aggregated Qualifying Data.")
        qualifying_table = self._db_builder.extract_qualifying_grain(self.fl_results_dir, self._start_date, self._end_date)
        print("Extracting Aggregated Results Data...")
        results_table = self._db_builder.extract_race_grain(self.fl_race_dir, self._start_date, self._end_date)
        print("Extracting Aggregated Telemetry Data...")
        race_telem_table = self._db_builder.extract_race_telemetry(self.fl_telem_dir, self._start_date, self._end_date)
        quali_telem_table = self._db_builder.extract_quali_telemetry(self.fl_telem_dir, self._start_date, self._end_date)
        print("Serializing pandas Tables into CSV...")
        extract_dt = self._builder.serialize_full(results_table, qualifying_table, season_table, race_telem_table, quali_telem_table)
        print("Upserting data into Postgres ")
        #initializing postgres client 
        postgres_client = self._postgres
        postgres_conn_uri = postgres_client.connection_factory(1, self._col,) # calling connection method to get connection string with 1 specified for the database developer privileges
        postgres_client.upsert_db(postgres_conn_uri, ti, self._inc_processed_dir, self._full_processed_dir, extract_dt)
        
        return

    # method to call to create pandas dataframes for incremental load - race by race
    elif decision == 2:
        
        print("-----------------------------------Data extraction, cleaning and conforming-----------------------------------------")
        print("Extracting Aggregated Qualifying Data.")
        qualifying_table = self._db_builder.increment_qualifying(self.fl_qualifying_dir, self._start_date, self._end_date)
        print("Extracting Aggregated Results Data...")
        results_table = self._db_builder.increment_results(self.fl_results_dir, self._start_date, self._end_date)
        print("Extracting aggregated Telemetry data... ")
        quali_telem_table = self._db_builder.increment_quali_telem(self.fl_telem_dir, self._start_date, self._end_date)
        race_telem_table = self._db_builder.increment_race_telem(self.fl_telem_dir, self._start_date, self._end_date)
        
        ti.xcom_push(key='results_table', value=results_table)
        ti.xcom_push(key='qualifying_table', value=qualifying_table)
        ti.xcom_push(key='race_telem_table', value=race_telem_table)
        ti.xcom_push(key='quali_telem_table', value=quali_telem_table)
        
        return
         
def load_wh(self):
    pass