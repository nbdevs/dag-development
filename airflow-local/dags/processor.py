""" Responsible for data extraction, cleaning and loading, as well as data transformations via sql procedure calls."""
from __future__ import annotations
from abc import ABC, abstractmethod
from connections import PostgresClient, SnowflakeClient, S3Client
from colours import Colours

# --------------------------------------------------------------------------------------------------
class Processor(ABC):
    """ Processor interface which creating different forms of data tables representing formula 1 data for the 3 different grains 
    in the system, aggregates them accordingly and passes the result to airflow to store in x-coms.
    Base class with empty functions for overriding."""
    
    from datetime import datetime 
    import pandas as pd
    
    @abstractmethod    
    def is_not_empty(self, s):
        pass

    @abstractmethod
    def determine_format(self):
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
    def incremental_qualifying(self, cache_dir, start_date, end_date) -> pd.DataFrame:
        pass
    
    @abstractmethod
    def incremental_results(self, cache_dir, start_date, end_date) -> pd.DataFrame:
        pass
    
    @abstractmethod
    def incremental_quali_telem(self, cache_dir, start_date, end_date) -> pd.DataFrame:
        pass
    
    @abstractmethod
    def incremental_race_telem(self, cache_dir, start_date, end_date) -> pd.DataFrame:
        pass
    
    @abstractmethod
    def serialize_full(self, user_home, race_table, driver_table, season_table, race_telem_table, quali_telem_table)-> datetime:
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
    
    import pandas as pd
    
    def __init__(self, col) -> pd.DataFrame:
        """Each new instance contains a colours class for formatting logging info."""
        self._col = col
    
    def is_not_empty(self, s, pathway):
        """ Function responsible for detecting presence of white space or emptiness in contents of a file.
        Will then be used to determine which pathway should be followed for ETL. 
        Returns Full or Incremental. """
        
        import os

        return bool(s and s.isspace() or os.stat(pathway).st_size==0)  
    
    def determine_format(self, pathway) -> str:
        """Function which determines if incremental or full load is necessary."""
 
        import logging
        
        #string holder for load type of the etl pipeline
        load_type = ["Full", "Incremental"]
        lines = ""
         
        with open(pathway, "r") as file:
            lines = file.readline()
        if self.is_not_empty(lines, pathway) == False:
            logging.info("INCREMENTAL LOAD chosen as extract format.")
            with open(pathway, "a") as writefile:
                var = load_type[1]
                writefile.write("{0}".format(var))
                writefile.write("\n")
            return var
        elif self.is_not_empty(lines, pathway) == True:
            logging.info("FULL LOAD chosen as extract format.")
            with open(pathway, "w") as wfile:
                variable = load_type[0]
                wfile.write("{0}".format(variable))
                wfile.write("\n")
            return variable
         
    def postgres_transformations(self):
        pass
        
    def extract_season_grain(self, cache_dir, start_date, end_date) -> pd.DataFrame:
        """ Extracts the season grain data from the api and aggregates them together ready for loading into the database.
            takes a target cache directory as the input for first variable,
            start date and end date for extraction of f1 data as the next two. """

        import logging
        import fastf1 as f1
        import pandas as pd
      
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
    
        #splitting the date and time apart
        season["race_date"] = pd.to_datetime(season['races_date']).dt.date
        season["race_time"] = pd.to_datetime(season["races_date"]).dt.time

        season.drop("races_date", axis=1, inplace=True)
        
        return season
        
    def extract_qualifying_grain(self, cache_dir, start_date, end_date) -> pd.DataFrame:
        """ Extracts the qualifying/driver aggregated data from the api and stores in the dataframe.
            Takes a target cache directory as the first variable
            start date and end date date for data extraction for the rest."""
        
        import logging 
        import fastf1 as f1 
        import pandas as pd
        import numpy as np
        
        logging.info("------------------------------Extracting Aggregated Qualifying Data--------------------------------------")
                
        f1.Cache.enable_cache(cache_dir) #enable cache for faster data retrieval
        ultimate = [] # empty list to store dataframe for all results across multiple seasons

        #outer for loop for seasons
        for s in range(start_date, end_date):
            size = f1.get_event_schedule(s)
            
            # get the number of races in the season by getting all race rounds greater than 0
            race_no = size.query("RoundNumber > 0").shape[0]
            print(race_no)
            race_name_list = [] # empty list for storing races 
            final = [] # empty list for aggregated race data for all drivers during season
    
            
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
                    
                     # drivers in a race
                driver_data = pd.concat(listd)
                final.append(driver_data)
            
            # all races in a season
            processed_data = pd.concat(final)
            ultimate.append(processed_data)
                            
        # multiple seasons
        quali_table = pd.concat(ultimate)
     
        return quali_table
        
    def extract_race_grain(self, cache_dir, start_date, end_date) -> pd.DataFrame:
        """ Extracts race/result aggregated data and stores in pandas dataframe"""
        
        import fastf1 as f1 
        import pandas as pd
        import numpy as np
        
        f1.Cache.enable_cache(cache_dir) # enable cache for faster data retrieval 
        ultimate = [] # empty list to store dataframe for all results across multiple seasons

        #outer for loop for seasons
        for s in range(start_date, end_date):
            size = f1.get_event_schedule(s)
            
            # get the number of races in the season by getting all race rounds greater than 0
            race_no = size.query("RoundNumber > 0").shape[0]
            print(race_no)
            race_name_list = [] # empty list for storing races 
            final = [] # empty list for aggregated race data for all drivers during season
            
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

                # drivers in a race
                driver_data = pd.concat(listd)
                final.append(driver_data)
    
            # all races in a season
            processed_data = pd.concat(final)
            ultimate.append(processed_data)
                    
        # multiple seasons
        race_table = pd.concat(ultimate)
                
       # f1.Cache.clear_cache(cache_dir) # clear cache to free disk space  
 
        return race_table
    
    def serialize_full(self, user_home, race_table, driver_table, season_table, race_telem_table, quali_telem_table):
        """ This method is responsible for serializing the files into csv format before they are uploaded in raw form to an s3 bucket for persistence."""
        import os 
        import logging
        from datetime import datetime 
        import pandas as pd 
        from decouple import config
        
        logging.info("------------------------------Serializing DataFrames to CSV--------------------------------------")
        
        extract_dt = datetime.today().strftime("%Y-%m-%d")

        dataframes = [race_table, driver_table, season_table, race_telem_table, quali_telem_table]
        
        i = 0 # counter variable 
        
        #append date to the end of the files, and output to the correct directory
        for d in dataframes:
            
            i += 1 # increment counter 
            
            if i == 1:
                d.to_csv('{}/cache/FullProcessed/Results{}.csv'.format(user_home, extract_dt), index=False, header=True)
                
            elif i == 2:
                d.to_csv('{}/cache/FullProcessed/Qualifying{}.csv'.format(user_home, extract_dt), index=False, header=True)
                
            elif i == 3:
                d.to_csv('{}/cache/FullProcessed/Season{}.csv'.format(user_home, extract_dt), index=False, header=True)
            
            elif i == 4:
                d.to_csv('{}/cache/FullProcessed/RaceTelem{}.csv'.format(user_home, extract_dt), index=False, header=True)
                
            elif i == 5:
                d.to_csv('{}/cache/FullProcessed/QualifyingTelem{}.csv'.format(user_home, extract_dt), index=False, header=True)
                
        return extract_dt # return the date to push to xcoms for s3 hook to identify the file by the date for uploading into postgres 
       
    def incremental_qualifying(self, cache_dir, start_date, end_date):
        """This function handles the incremental extraction of qualifying data into the database"""
        
        import logging
        import os
        from datetime import datetime 
        from decouple import config
        
        extract_dt = datetime.today().strftime("%Y-%m-%d")
        cache_dir = config("inc_qualifying")
        cache_dp = cache_dir.format(extract_dt)
        
        pass
    
    def incremental_results(self, cache_dir, start_date, end_date):
        """This function handles the incremental loading of race result data into the database"""
        
        import logging
        import os
        from datetime import datetime 
        from decouple import config
        
        extract_dt = datetime.today().strftime("%Y-%m-%d")
        cache_dir = config("inc_results")
        cache_dp = cache_dir.format(extract_dt)
        
        pass
    
    def incremental_quali_telem(self, cache_dir, start_date, end_date):
        import logging
        import os
        from datetime import datetime 
        from decouple import config
        
        extract_dt = datetime.today().strftime("%Y-%m-%d")
        cache_dir = config("inc_qualitelem")
        cache_dp = cache_dir.format(extract_dt)
        
        pass
    
    def incremental_race_telem(self, cache_dir, start_date, end_date):
        import logging
        import os
        from datetime import datetime 
        from decouple import config 
        
        extract_dt = datetime.today().strftime("%Y-%m-%d")
        cache_dir = config("inc_racetelem")
        cache_dp = cache_dir.format(extract_dt)
        
        pass
    
    def increment_serialize(self, qualifying_table, results_table, race_telem_table, quali_telem_table):
        
        import logging
        import os
        from datetime import datetime 
        from decouple import config
        
        logging.info("------------------------------Serializing DataFrames to CSV--------------------------------------")
        
        extract_dt = datetime.today().strftime("%Y-%m-%d")

        dataframes = [qualifying_table, results_table, race_telem_table, quali_telem_table]
        user_home = config("user_home")
        
        i = 0 # counter variable 
        
        #append date to the end of the files, and output to the correct directory
        for d in dataframes:
            
            i += 1 # increment counter 
            
            if i == 1:
                d.to_csv('Users/{}/cache/IncrementalProcessed/Qualifying{}.csv'.format(user_home, extract_dt), index=False, header=True)
                
            elif i == 2:
                d.to_csv('Users/{}/cache/IncrementalProcessed/Results{}.csv'.format(user_home, extract_dt), index=False, header=True)
                
            elif i == 3:
                d.to_csv('/Users/{}/cache/IncrementalProcessed/RaceTelem{}.csv'.format(user_home, extract_dt), index=False, header=True)
                
            elif i == 4:
                d.to_csv('/Users/{}/cache/IncrementalProcessed/QualifyingTelem{}.csv'.format(user_home, extract_dt), index=False, header=True)
                
        return extract_dt
     
    def extract_quali_telem(self, cache_dir, start_date, end_date) -> pd.DataFrame:
        """ Full load of telemetry data for qualifying for each event of the race calendar.
            Takes the cache directory, start and end date for data load as parameters, and returns a dataframe."""
          
        import pandas as pd
        import fastf1 as f1
        import numpy as np

        f1.Cache.enable_cache(cache_dir) # enabling cache for faster data retrieval 
        ultimate = [] # empty list to store dataframe for all results across multiple seasons

        #outer for loop for seasons
        for s in range(start_date, end_date):
            size = f1.get_event_schedule(s)
            # get the number of races in the season by getting all race rounds greater than 0
            race_no = size.query("RoundNumber > 0").shape[0]
            race_name_list = [] # empty list for storing races 
            final = [] # empty list for aggregated race data for all drivers during season
    
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
                
                # selecting columns to be dropped from weather df
                col = ["Time", "AirTemp", "Pressure", "WindDirection"]
                for c in col:
                    weather_data.drop(c, axis=1, inplace=True)
                    weather_data = weather_data.reset_index(drop=True)

                drivers = session.drivers #list of drivers in the session
                listd = []
                series = []
                driver_list = []
                
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
                # drivers in a race
                drivers_data = pd.concat(driver_list)
                final.append(drivers_data)
            
            # all races in a season
            processed_data = pd.concat(final)
            ultimate.append(processed_data)
            
        quali_telemetry_table = pd.concat(ultimate)    
        
        column = ["SpeedST", "IsPersonalBest", "PitOutTime", "PitInTime", "TrackStatus","LapStartTime", "LapStartDate", "Sector1SessionTime", "FreshTyre", "Time", "Sector2SessionTime", "Sector3SessionTime", "SpeedI1", "SpeedI2", "WindSpeed", "SpeedFL", "DistanceToDriverAhead"]
        #drop irrelevant columns 
        for c in column:
            quali_telemetry_table.drop(c, axis=1, inplace=True)
        
        #convert time deltas to strings and reformat 
        col=["LapTime", "Sector1Time", "Sector2Time", "Sector3Time", "pole_lap"]
        for c in col:
            quali_telemetry_table[c] = quali_telemetry_table[c].astype(str).map(lambda x: x[10:])

        #replace all empty values with NaN
        quali_telemetry_table.replace(r'^\s*$', np.nan, regex=True, inplace=True)

        #provide desired column names 
        quali_telemetry_table.columns = ["car_no", "lap_time", "lap_no", "s1_time", "s2_time", "s3_time", "compound", "tyre_life", "race_stint", "team_name", "driver_identifier", "IsAccurate", "season_year", "race_name", "pole_lap", "humidity", "occur_of_rain_quali", "track_temp", "revs_per_min", "car_speed", "gear_no", "throttle_pressure"]

        return quali_telemetry_table
    
    def extract_race_telem(self, cache_dir, start_date, end_date) -> pd.DataFrame:
        """Full load of telemetry data for qualifying for each event of the race calendar.
            Takes the cache directory, start and end date for data load as parameters, and returns a dataframe."""
        
        import pandas as pd
        import fastf1 as f1
        import numpy as np

        f1.Cache.enable_cache(cache_dir) #enabling cache for faster data retrieval 
        ultimate = [] # empty list to store dataframe for all results across multiple seasons

        #outer for loop for seasons
        for s in range(start_date, end_date):
            size = f1.get_event_schedule(s)
            # get the number of races in the season by getting all race rounds greater than 0
            race_no = size.query("RoundNumber > 0").shape[0]
            race_name_list = [] # empty list for storing races 
            final = [] # empty list for aggregated race data for all drivers during season
            
            # get all the names of the races for this season
            for i in range(1, race_no):

                event = size.get_event_by_round(i)
                race_name = event.loc["EventName"] # access by column to get value of race
                race_name_list.append(race_name) 
                session = f1.get_session(s, i, 'R')
                
                # load all driver information for session
                session.load(telemetry=True, laps=True, weather=True)
                
                # testing if telemetry data has been loaded
                try:
                    session.laps.pick_driver("HAM").get_telemetry() 
                except Exception as exc:
                    print(f'"Telemetry data not available for {race_name}"')
                    continue # move to the next race 

                #load weather data
                weather_data = session.laps.get_weather_data()
                    
                # selecting columns to be dropped from weather df
                col = ["Time", "AirTemp", "Pressure", "WindDirection"]
                for c in col:
                    weather_data.drop(c, axis=1, inplace=True)
                    weather_data = weather_data.reset_index(drop=True)

                drivers = session.drivers
                listd = []
                series = []
                driver_list = []
                
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
                # drivers in a race
                drivers_data = pd.concat(driver_list)
                final.append(drivers_data)
            
            # all races in a season
            processed_data = pd.concat(final)
            ultimate.append(processed_data)
            
        race_telemetry_table = pd.concat(ultimate)
                         
        column = ["TrackStatus","LapStartTime", "LapStartDate", "Sector1SessionTime", "FreshTyre", "Time", "Sector2SessionTime", "Sector3SessionTime", "SpeedI1", "SpeedI2", "WindSpeed", "SpeedFL", "SpeedST"]
        #drop irrelevant columns 
        for c in column:
            race_telemetry_table.drop(c, axis=1, inplace=True)
        
        #convert time deltas to strings and reformat 
        col=["LapTime", "Sector1Time", "Sector2Time", "Sector3Time", "fastest_lap"]
        for c in col:
            race_telemetry_table[c] = race_telemetry_table[c].astype(str).map(lambda x: x[10:])

        #replace all empty values with NaN
        race_telemetry_table.replace(r'^\s*$', np.nan, regex=True, inplace=True)

        #provide desired column names 
        race_telemetry_table.columns =["car_no", "lap_time", "lap_no", "time_out", "time_in", "s1_time", "s2_time", "s3_time", "IsPersonalBest", "compound", "tyre_life", "race_stint", "team_name", "driver_identifier", "IsAccurate", "season_year", "race_name", "fastest_lap", "pit_duration", "humidity", "occur_of_rain_race", "track_temp",  "revs_per_min", "car_speed", "gear_no", "throttle_pressure"]

        return race_telemetry_table
        
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
    
    from datetime import datetime
    import pandas as pd
    
    def __init__(self, col) -> pd.DataFrame:
        """Each new instance contains a colours class used for formatting."""
        self._col = col
         
    def is_not_empty(s):
        pass

    def determine_format(self):
        pass
    
    def extract_season_grain(self, cache_dir, start_date, end_date) -> pd.DataFrame:
        pass
       
    def extract_qualifying_grain(self, cache_dir, start_date, end_date) -> pd.DataFrame:
        pass
    
    def extract_race_grain(self, cache_dir, start_date, end_date) -> pd.DataFrame:
        pass

    def incremental_qualifying(self, cache_dir, start_date, end_date) -> pd.DataFrame:
        pass
    
    def incremental_results(self, cache_dir, start_date, end_date) -> pd.DataFrame:
        pass
    
    def incremental_quali_telem(self, cache_dir, start_date, end_date) -> pd.DataFrame:
        pass
    
    def incremental_race_telem(self, cache_dir, start_date, end_date) -> pd.DataFrame:
        pass
    
    def serialize_full(self, user_home, race_table, driver_table, season_table, race_telem_table, quali_telem_table)-> datetime:
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
