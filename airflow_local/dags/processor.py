""" Responsible for data extraction, cleaning and loading, as well as data transformations via sql procedure calls."""
from __future__ import annotations
from abc import ABC, abstractmethod
from connections import PostgresClient, SnowflakeClient, S3Client
from colours import Colours
from airflow import DAG
# --------------------------------------------------------------------------------------------------


class Processor(ABC):
    """ Processor interface which creating different forms of data tables representing formula 1 data for the 3 different grains 
    in the system, aggregates them accordingly and passes the result to airflow to store in x-coms.
    Base class with empty functions for overriding."""

    from datetime import datetime
    import pandas as pd

    @abstractmethod
    def path_generator(self, load_type, home_dir, network_scope, pathway):
        pass
    
    @abstractmethod
    def cache_cleaner(self, load_type, home_dir, network_scope, pathway):
        pass
    
    @abstractmethod
    def is_not_empty(self, s):
        pass

    @abstractmethod
    def extract_year(self, date_obj, dt_format):
        pass

    @abstractmethod
    def validate_pathway_file(self, file):
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
    def csv_producer(self, csv_dir, extract_dt, dataframes,  table_name, extract_type):
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
        self._dt_format = '%Y-%m-%d'
    
    def path_generator(self, load_type, home_dir, network_scope, pathway) -> list[str]:
        """  This function is responsible for generating the file paths for all the directories 
        that are going to be cleaned up after ETL. 
        Takes four inputs:
        -load_type refers to full or incremental load
        -home_dir refers to the file path for home directory on local machine or on linux machine
        -network scope refers to whether the cleanup is taking place locally or on linux machine
        -pathway refers to whether cache files or log files are being cleaned - with cache files needing to be cleaned more 
        frequently distinction was needed between them.
        Returns a list of strings containing all the file paths. """
        
        paths = [] # outer array to store the baseline file paths for both logs and cache files
        if pathway == "cache": # if the path says to clean up cache files 
            if network_scope == "local": # ascertain whether this is happen locally and generate file paths
                for i in range(0,len(load_type)):
                    paths.append(f'{home_dir}/{load_type[i]}/')     #load type refers to a list which differs for both logs and cache file cleanup
            elif network_scope == "cloud": # or on linux machine and generate file paths
                for i in range(0,len(load_type)):
                    paths.append(f'{home_dir}/{load_type[i]}/')
            
            complete_paths = [] # outer array
            # arrays containing the names of subfolders within main directory
            full_cache_array = ["Results", "Season", "Qualifying", "Telemetry"]
            incremental_cache_array = ["Qualifying", "QualiTelem", "RaceTelem", "Results"]

            for i in range(0,len(full_cache_array)):
                # join the subdirectories to the main dirrctories to create the complete file paths.
                complete_paths.append(paths[0] + full_cache_array[i])
                complete_paths.append(paths[1] + incremental_cache_array[i])
                if i == 2: # when it is cleaning up the FullProcessed directory perform these extra steps
                    complete_paths.append(paths[0] + full_cache_array[i])
                    complete_paths.append(paths[1] + incremental_cache_array[i])
                    complete_paths.append(paths[i])
                if i == 3: # when cleaning up IncrementalProcessed directory perform these extra steps 
                    complete_paths.append(paths[0] + full_cache_array[i])
                    complete_paths.append(paths[1] + incremental_cache_array[i])
                    complete_paths.append(paths[i])
            
            return complete_paths
        
        elif pathway == "logs": # cleanup of log files/folders
            if network_scope == "local": # ascertain whether this is happening locally and generate file paths
                for i in range(0,len(load_type)):
                    paths.append(f'{home_dir[0]}/{load_type[i]}/')    
            elif network_scope == "cloud": # or on linux machine and generate file paths 
                for i in range(0,len(load_type)):
                    paths.append(f'{home_dir[1]}/{load_type[i]}/') # load type refers to a list fo subdirectories in the logs folder
            
            return paths
        
    def cache_cleaner(self, load_type, home_dir, network_scope, pathway) -> None:
        """This function performs the cleaning of the cache folder. 
        Takes four inputs:
        -load_type: a list containing subdirectories within the cache folder to clean up 
        -home_dir: the path to the home directory on the machine
        -network_scope: determines whether cleanup is happening locally or on linux machine
        -pathway: determines whether a cache or log clean is occurring in the path generator function"""
        
        import os 
        import shutil 
        
        paths = self.path_generator(load_type, home_dir, network_scope, pathway) #function call to generate list of directories.
            
        for i in range(0, len(paths)):
            cache_files = os.listdir(paths[i]) #turn paths into directory tree
            for f in cache_files: #iterate through each folder/file in directory
                if os.path.isfile(f) and not "DS_Store" in f: # if item is a file 
                    try:
                        new_file = paths[i] + "/" + f # create the full file path
                        os.remove(new_file) # remove file from the folder
                    except FileNotFoundError:
                        shutil.rmtree(new_file)
                elif not os.path.isfile(f) and "DS_Store" not in f: # if item not a file then it is a folder 
                    try:
                        new_file = paths[i] + "/" + f # create full folder path
                        shutil.rmtree(new_file) # remove folder contents recursively 
                    except NotADirectoryError as e: # if it not a folder, is it a csv or .log file
                        os.remove(new_file)
        return
            
    def is_not_empty(self, s, pathway):
        """ Function responsible for detecting presence of white space or emptiness in contents of a file.
        Will then be used to determine which pathway should be followed for ETL. 
        Returns Full or Incremental. """

        import os

        return bool(s and s.isspace() or os.stat(pathway).st_size == 0)

    def extract_year(self, date_obj, dt_format):
        """ This function extracts the year from a datetime object as a string and returns it in integer form.
        Args:
        date_obj (datetime): datetime object which represents the extract date.
        dt_format (string): string object representing the format of the date string."""

        from datetime import datetime

        date_str = date_obj.strftime(dt_format)
        year = int(date_str[:4])
        return year

    def validate_pathway_file(self, file):
        """Examines pathway.txt file for the last known load path to determine which task_id is used for extarct_dt

        Args:
            file (textfile): pathway script which monitors the load pathway taken.
        """
        import logging

        pathway_array = []

        with open(file, "r") as r:
            for line in r:
                pathway_array.append(line)
        try:
            assert len(pathway_array) != 0, "File contents empty."
            last_line = pathway_array[-1]

        except AssertionError as err:
            logging.error(err)
            exit

        return last_line

    def determine_format(self, pathway) -> str:
        """Function which determines if incremental or full load is necessary."""

        import logging

        # string holder for load type of the etl pipeline
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
        
    def determine_load_date(self, pathway) -> str:
        """function which determines which load_type variable is produced for incremental upsert"""
        
        import logging 
    
        with open(pathway, "r") as file:
            lines = file.readlines()
            if len(lines) == 2 and lines[1] == "Incremental": # the first incremental after a full should use the full parameter
                return "Full"
            elif len(lines) > 2 and lines[2] == "Incremental": # every incremental thereafter uses the incremental parameter
                return "Incremental"
                
    def postgres_transformations(self):
        pass

    def extract_season_grain(self, cache_dir, start_date, end_date) -> pd.DataFrame:
        """ Extracts the season grain data from the api and aggregates them together ready for loading into the database.
            takes a target cache directory as the input for first variable,
            start date and end date for extraction of f1 data as the next two. """

        import fastf1 as f1 
        import requests as r 
        import json 
        import logging
        import pandas as pd
        
        #Initialising variables and arrays
        event = 'results'
        race_list = []
        track_holder = []
        j = 1 # counter variable
        # enable cache for faster data retrieval
        f1.Cache.enable_cache(cache_dir)

        logging.info("------------------------------Extracting Aggregated Season Data--------------------------------------")

        #outer for loop for seasons
        for s in range(start_date, end_date):
            size = f1.get_event_schedule(s)
            
            # get the number of races in the season by getting all race rounds greater than 0
            race_no = size.query("RoundNumber > 0").shape[0]
      
            #get all the names of the races for this season
            for i in range(1, race_no):
                # string url to produce json response by appending .json with request 
                race_string = 'https://ergast.com/api/f1/{0}/{1}/{2}.json?limit=1000'.format(s, i, event)
                
                # exception handling for requests 
                try:
                    # get response object then convert to text to then load to python dict obj
                    race = r.get(race_string, headers={'Authorization':'{0}' '{1}'.format('Accept', 'application/json'), 'Content-Type': 'application/json'})
                    race.raise_for_status() # generate status code to see if successful request recieved
                    try:
                        t = race.text # generate text from response object
                        race_j = json.loads(t) # load to python dict obj 
                    except r.exceptions.JSONDecodeError as e:
                        print("Empty JSON response.")
                        logging.warning(e)
                    
                except r.exceptions.HTTPError as http_err:
                    print("Status code exceeded acceptable range: 200 <= x < 300 [Status code", race.status_code, "]")
                    logging.warning(http_err)
            
                except ConnectionError as conn_err:
                    logging.warning(conn_err)
            
                else:     
            
                    if not race_j["MRData"]: # dict is empty
                        logging.critical("Empty JSON")
                        
                    else: # dict is not empty 
            
                        try: # get the name of the race to indicate a valid repsonse
        
                            season_year = race_j["MRData"]["RaceTable"]["season"]
                            race_round = race_j["MRData"]["RaceTable"]["round"]
                
                            for i in range (0, len(race_j["MRData"]["RaceTable"]["Races"])): # get data for each race weekend
                                race_date = race_j["MRData"]["RaceTable"]["Races"][i]["date"]
                                race_t= race_j["MRData"]["RaceTable"]["Races"][i]["time"]
                                race_time = race_t.replace("Z", "")
                                country = race_j["MRData"]["RaceTable"]["Races"][i]["Circuit"]["Location"]["country"]
                                city = race_j["MRData"]["RaceTable"]["Races"][i]["Circuit"]["Location"]["locality"]
                                race_name = race_j["MRData"]["RaceTable"]["Races"][i]["raceName"]  
                                track_name = race_j["MRData"]["RaceTable"]["Races"][i]["Circuit"]["circuitName"]
                        
                                race_n = {"season_year" : season_year , "race_name": race_name, "track_name": track_name, "race_round" : race_round, "race_date" : race_date, 
                                "race_time": race_time, "country": country, "city": city} # sort data into dict 
                                track_holder.append(race_n) # append dict to array 

                                j+=1 # increment counter variable
                        except KeyError as msg:
                            logging.error(msg)

        season = pd.DataFrame(track_holder)
        season.index +=1
        
        return season
        

    def extract_qualifying_grain(self, cache_dir, start_date, end_date) -> pd.DataFrame:
        """ Extracts the qualifying/driver aggregated data from the api and stores in the dataframe.
            Takes a target cache directory as the first variable
            start date and end date date for data extraction for the rest."""
            
        import fastf1 as f1 
        import requests as r 
        import json 
        import logging
        import pandas as pd 

        # Initialising variables and arrays
        event = 'qualifying'
        race_list = []
        track_holder = []
        j = 1 # counter variable
        f1.Cache.enable_cache(cache_dir) # enable cache for faster data retrieval

        logging.info(
            "------------------------------Extracting Aggregated Qualifying Data--------------------------------------")

        #outer for loop for seasons
        for s in range(start_date, end_date):
            size = f1.get_event_schedule(s)
            
            # get the number of races in the season by getting all race rounds greater than 0
            race_no = size.query("RoundNumber > 0").shape[0]
            race_name_list = [] # empty list for storing races 
            final = []

            #get all the names of the races for this season
            for i in range(1, race_no):
                # string url to produce json response by appending .json with request 
                race_string = 'https://ergast.com/api/f1/{0}/{1}/{2}.json?limit=1000'.format(s, i, event)
                
                # exception handling for requests 
                try:
                    # get response object then convert to text to then load to python dict obj
                    race = r.get(race_string, headers={'Authorization':'{0}' '{1}'.format('Accept', 'application/json'), 'Content-Type': 'application/json'})
                    race.raise_for_status() # generate status code to see if successful request recieved
                    try:
                        t = race.text # generate text from response object
                        race_j = json.loads(t) # load to python dict obj 
                    except r.exceptions.JSONDecodeError as e:
                        print("Empty JSON response.")
                        logging.warning(e)
                        break
                    
                except r.exceptions.HTTPError as http_err:
                    print("Status code exceeded acceptable range: 200 <= x < 300 [Status code", race.status_code, "]")
                    logging.warning(http_err)
            
                except ConnectionError as conn_err:
                    logging.warning(conn_err)
            
                else:     
            
                    if not race_j["MRData"]: # dict is empty
                        logging.critical("Empty JSON")
                        
                    else: # dict is not empty 
            
                        try: # get the name of the race to indicate a valid response
        
                            season_year = race_j["MRData"]["RaceTable"]["season"]
                            race_round = race_j["MRData"]["RaceTable"]["round"]
                
                            for i in range (0, len(race_j["MRData"]["RaceTable"]["Races"])): # get data for each race weekend
                                quali_date = race_j["MRData"]["RaceTable"]["Races"][i]["date"]
                                quali_t= race_j["MRData"]["RaceTable"]["Races"][i]["time"]
                                quali_time = quali_t.replace("Z", "")
                                country = race_j["MRData"]["RaceTable"]["Races"][i]["Circuit"]["Location"]["country"]
                                city = race_j["MRData"]["RaceTable"]["Races"][i]["Circuit"]["Location"]["locality"]
                                race_name = race_j["MRData"]["RaceTable"]["Races"][i]["raceName"]  
                                track_name = race_j["MRData"]["RaceTable"]["Races"][i]["Circuit"]["circuitName"]
                        
                                for j in range (0, len(race_j["MRData"]["RaceTable"]["Races"][i]["QualifyingResults"])): # get qualifying results for each driver in a race weekend
                                    quali_pos = race_j["MRData"]["RaceTable"]["Races"][i]["QualifyingResults"][j]["position"]
                                    car_no = race_j["MRData"]["RaceTable"]["Races"][i]["QualifyingResults"][j]["Driver"]["permanentNumber"]
                                    driver_id = race_j["MRData"]["RaceTable"]["Races"][i]["QualifyingResults"][j]["Driver"]["code"]
                                    forename = race_j["MRData"]["RaceTable"]["Races"][i]["QualifyingResults"][j]["Driver"]["givenName"]
                                    surname = race_j["MRData"]["RaceTable"]["Races"][i]["QualifyingResults"][j]["Driver"]["familyName"]
                                    driver_nationality = race_j["MRData"]["RaceTable"]["Races"][i]["QualifyingResults"][j]["Driver"]["nationality"]
                                    date_of_birth = race_j["MRData"]["RaceTable"]["Races"][i]["QualifyingResults"][j]["Driver"]["dateOfBirth"]
                                    team_name = race_j["MRData"]["RaceTable"]["Races"][i]["QualifyingResults"][j]["Constructor"]["name"]
                                    team_nationality = race_j["MRData"]["RaceTable"]["Races"][i]["QualifyingResults"][j]["Constructor"]["nationality"]
                                    try:
                                        best_q1 = race_j["MRData"]["RaceTable"]["Races"][i]["QualifyingResults"][j]["Q1"]
                                    except KeyError:
                                        logging.error(f' Q1 data not available for {forename} {surname} - Car {car_no} - for the {race_name}.')
                                        best_q1 = '0:00.000'
                                    try:
                                        best_q2 = race_j["MRData"]["RaceTable"]["Races"][i]["QualifyingResults"][j]["Q2"]
                                    except KeyError:
                                        logging.error(f' Q2 data not available for {forename} {surname} - Car {car_no} - for the {race_name}.')
                                        best_q2 = '0:00.000'
                                    try:
                                        best_q3 = race_j["MRData"]["RaceTable"]["Races"][i]["QualifyingResults"][j]["Q3"]
                                    except KeyError:
                                        logging.error(f' Q3 data not available for {forename} {surname} - Car {car_no} - for the {race_name}.')
                                        best_q3 = '0:00.000'
                                        
                                    quali_n = {"season_year" : season_year , "race_name" : race_name, "track_name": track_name, "race_round" : race_round, "quali_date" : quali_date, 
                                "quali_time": quali_time, "country": country, "city" : city, "quali_pos" : quali_pos, "car_no" : car_no, "driver_id" : driver_id, "forename" : forename, "surname": surname, 
                                            "driver_nationality" : driver_nationality, "date_of_birth": date_of_birth, "team_name" : team_name, "team_nationality": team_nationality, "best_q1": best_q1,
                                            "best_q2" : best_q2, "best_q3" : best_q3} # sort data into dict 
                                    track_holder.append(quali_n) # append dict to array 
                                
                                j+=1 # increment counter variable
                        except ValueError as msg:
                            logging.critical(msg)

        qualifying_table = pd.DataFrame(track_holder)
        qualifying_table.index +=1

        return qualifying_table

    def extract_race_grain(self, cache_dir, start_date, end_date) -> pd.DataFrame:
        """ Extracts race/result aggregated data and stores in pandas dataframe"""

        import fastf1 as f1
        import pandas as pd
        import numpy as np
        import logging

        # enable cache for faster data retrieval
        f1.Cache.enable_cache(cache_dir)
        ultimate = []  # empty list to store dataframe for all results across multiple seasons

        # outer for loop for seasons
        for season in range(start_date, end_date):
            event_schedule = f1.get_event_schedule(season)

            # get the number of races in the season by getting all race rounds greater than 0
            race_no = event_schedule.query("RoundNumber > 0").shape[0]

            race_name_list = []  # empty list for storing races
            final = []  # empty list for aggregated race data for all drivers during season

            # get all the names of the races for this season
            for race in range(1, race_no):

                jam = f1.get_event(season, race)
                # access by column to get value of race
                race_name = jam.loc["EventName"]
                race_name_list.append(race_name)

                ff1 = f1.get_session(season, race, 'R')
                # load all driver information for session
                ff1.load()
                drivers = ff1.drivers
                listd = []

                # loop through every driver in the race
                for driver in drivers:

                    name = ff1.get_driver(driver)
                    newname = name.to_frame().T  # invert columns and values

                    columns = ["BroadcastName", "FullName", "Q1", "Q2", "Q3", "Abbreviation",
                               "CountryCode", "TeamId", "DriverId", "TeamColor", "HeadshotUrl", "ClassifiedPosition"]

                    logging.info("Dropping columns...")
                    # drop irrelevant columns
                    for c in columns:
                        newname.drop(c, axis=1, inplace=True)

                    # provide desired column names
                    newname.columns = ["car_number", "team_name", "forename", "surname",
                                       "finish_pos", "start_pos", "race_time", "driver_status_update", "race_points"]
                    # provide new index
                    newname.index.name = "driver_id"

                    try:
                        newname["finish_pos"] = pd.to_numeric(newname["finish_pos"])
                        newname["pos_change"] =  newname["start_pos"] - newname["finish_pos"]
                        newname["season_year"] = season
                        newname["race_name"] = race_name
                    except ValueError:
                        logging.error(f' Classification data not available for {newname.iloc[-1]["forename"]} {newname.iloc[-1]["surname"]} - Car {newname.iloc[-1]["car_number"]} due to retirement from the {race_name}.')
                        newname["pos_change"] = "R"
                        newname["season_year"] = season
                        newname["race_name"] = race_name
                    
                    # convert time deltas to strings and reformat
                    col = ["race_time"]

                    for c in col:
                        newname[c] = newname[c].astype(
                            str).map(lambda x: x[10:])

                    # replace all empty values with 0
                    newname.replace(r'^\s*$', 0.0, regex=True, inplace=True)
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

    def csv_producer(self, user_home, extract_dt, dataframe, table_name, extract_type):
        """
        This function generates the CSV files for the tabled data for all season, race and telemetry level of granularity.
        Takes 5 inputs, one being the user home directory which is a reference to the location the csv files will be stored, 
        extract date which attaches the date fo data extraction to the end of the cv file
        dataframe which refers to the pandas dataframe containing the data
        table_name which refers to the database table name of the data,
        extract_type which refers to whether this is occurring on a full or incremental load."""

        if table_name == "Results":
            dataframe.to_csv('{}/cache/{}Processed/{}-{}.csv'.format(user_home, extract_type, table_name,
                        extract_dt), index=False, encoding='utf-8', header=True)
        elif table_name == "Qualifying":
            dataframe.to_csv('{}/cache/{}Processed/{}-{}.csv'.format(user_home, extract_type, table_name,
                        extract_dt), index=False, encoding='utf-8', header=True)
        elif table_name == "Season":
            dataframe.to_csv('{}/cache/{}Processed/{}-{}.csv'.format(user_home, extract_type, table_name,
                extract_dt), index=False, encoding='utf-8', header=True)
        elif table_name == "RaceTelem":
            dataframe.to_csv('{}/cache/{}Processed/{}-{}.csv'.format(user_home, extract_type, table_name,
                        extract_dt), index=False, encoding='utf-8', header=True)
        elif table_name == "QualifyingTelem":
            dataframe.to_csv('{}/cache/{}Processed/{}-{}.csv'.format(user_home, extract_type, table_name,
                        extract_dt), index=False, encoding='utf-8', header=True)
        return

    def incremental_qualifying(self, ti):
        """This function handles the incremental extraction of qualifying data into the database.
            Returns a string array containing data for all drivers for either a single or multiple qualifying sessions."""

        import logging
        from datetime import datetime
        from decouple import config
        import fastf1 as f1
        import requests as r
        import json
        import logging

        # setting logging parameters
        logging.basicConfig(filename='', format='%(asctime)s %(message)s',
                            encoding='utf-8', level=logging.ERROR)
        logging.basicConfig(
            filename='', format='%(asctime)s %(message)s', encoding='utf-8', level=logging.INFO)

        # setting parameters
        round_no = 1
        event = 'qualifying'
        race_data = []
        path = config("pathway")

        # setting extract parameters
        if self.validate_pathway_file(path) == 'Full':
            extract_dt = ti.xcom_pull(
                task_ids='full_load_serialization', key='extract_date')
        elif self.validate_pathway_file(path) == 'Incremental':
            extract_dt = ti.xcom_pull(
                task_ids='inc_load_serialization', key='incremental_extract_date')

        dt_format = self._dt_format

        # getting season year from extract_dt param
        season = self.extract_year(extract_dt, dt_format)

        while round_no != 0:

            # string url to produce json response by appending .json with request
            race_string = 'https://ergast.com/api/f1/{0}/{1}/{2}.json?limit=1000'.format(
                season, round_no, event)

            # exception handling for requests
            try:
                # get response object then convert to text to then load to python dict obj
                race = r.get(race_string, headers={'Authorization': '{0}' '{1}'.format(
                    'Accept', 'application/json'), 'Content-Type': 'application/json'})
                race.raise_for_status()  # generate status code to see if successful request received
                try:
                    t = race.text  # generate text from response object
                    race_json = json.loads(t)  # load to python dict obj
                except ValueError as e:
                    print("Empty JSON response.")
                    logging.warning(e)
                    break

            except race.exceptions.HTTPError as http_err:
                print(
                    "Status code exceeded acceptable range: 200 <= x < 300 [Status code", race.status_code, "]")
                logging.warning(http_err)

            except race.exceptions.ConnectionError as conn_err:
                logging.warning(conn_err)

            except race.exceptions.Timeout as timeout_err:
                logging.warning(timeout_err)

            else:

                if not race_json["MRData"]:  # dict is empty
                    logging.critical("Empty JSON")

                else:  # dict is not empty

                    try:  # get the name of the race to indicate a valid response

                        # check for empty json response object for race key
                        assert race_json['MRData']['RaceTable']['Races'] != [
                        ], "Race number exceeded season boundary. Data now up to date."

                        # loop through race array
                        for race in race_json['MRData']['RaceTable']['Races']:

                            race_date_str = race['date']
                            # formatting into datetime obj
                            race_date = datetime.strptime(
                                race_date_str, dt_format)

                            if race_date > extract_dt:  # if date of race is after last extract date
                                race_season = race['season']

                                for quali in race['QualifyingResults']:

                                    driver_name = quali['Driver']['familyName']
                                    driver_no = quali['number']
                                    driver_identifier = quali['Driver']['code']
                                    quali_pos = quali['position']
                                    team_name = quali['Constructor']['constructorId']

                                    try:
                                        assert quali['Q1'] != [
                                        ], "Driver did not participate in Q1."

                                        q1_time = quali['Q1']

                                        try:
                                            assert quali['Q2'] != [
                                            ], "Driver did not participate in Q2."
                                            q2_time = quali['Q2']

                                            try:
                                                assert quali['Q3'] != [
                                                ], "Driver did not participate in Q3."
                                                q3_time = quali['Q3']

                                            except KeyError as msg:
                                                logging.error(msg)
                                                logging.info(
                                                    "Driver did not participate in session. Setting as 0.")
                                                q3_time = 0

                                        except KeyError as msg:
                                            logging.error(msg)
                                            logging.info(
                                                "Driver did not participate in session. Setting as 0.")
                                            q2_time = 0

                                    except KeyError as msg:
                                        logging.error(msg)
                                        logging.info(
                                            "Driver did not participate in session. Setting as 0.")
                                        q1_time = 0

                                    race_n = {"race_id": race['raceName'], "quali_date": race['date'], "season_year": race_season,
                                              "driver_name": driver_name, "driver_no": driver_no, "driver_identifier": driver_identifier,
                                              "quali_pos": quali_pos, "team_name": team_name, "q1_time": q1_time, "q2_time": q2_time,
                                              "q3_time": q3_time}  # add race name, race date to tuple

                                    # adding quali result data to list
                                    race_data.append(race_n)

                            # get race date from json, change to datetime object and compare to extract_dt
                            # if after date then initiate download of data.

                            else:
                                logging.info(
                                    "Data exists already - race date precedes extract date. Skipping.")

                            round_no += 1  # increment counter for while loop

                    except AssertionError as e:
                        logging.info(e)
                        break

        return race_data

    def incremental_results(self, ti):
        """ Extracts race result data from Fastf1 api, for the incremental load pathway. 
        Args: 
        ti : type=task_instance - responsible for accessing the current running task instance for xcoms data."""

        import requests as r
        import json
        import logging
        from datetime import datetime
        from decouple import config
        import fastf1 as f1

       # subfunction for unavailable data
        def debug_print(baddata, msg='Data Unavailable for Driver'):
            # this line just makes it easier to read
            itemized = '\n'.join([f'\t{k}:{v}' for k, v in baddata.items()])
            print(f'Problem: {msg}\n{itemized}')

        # setting logging parameters
        logging.basicConfig(filename='', format='%(asctime)s %(message)s',
                            encoding='utf-8', level=logging.ERROR)
        logging.basicConfig(
            filename='', format='%(asctime)s $(message)s', encoding='utf-8', level=logging.INFO)

        # setting parameters
        round_no = 1
        event = 'results'
        race_list = []
        dt_format = self._dt_format
        path = config("pathway")

        # setting extract parameters
        if self.validate_pathway_file(path) == 'Full':
            extract_dt = ti.xcom_pull(
                task_ids='full_ext_load_race.full_qualifying_load', key='extract_date')
        elif self.validate_pathway_file(path) == 'Incremental':
            extract_dt = ti.xcom_pull(
                task_ids='incremental_ext_load_race.inc_qualifying_load', key='incremental_extract_date')

        # getting season year from extract_dt param
        season = self.extract_year(extract_dt, dt_format)

        while round_no != 0:
            # string url to produce json response by appending .json with request
            race_string = 'https://ergast.com/api/f1/{0}/{1}/{2}.json?limit=1000'.format(
                season, round_no, event)

            # exception handling for requests
            try:
                # get response object then convert to text to then load to python dict obj
                race = r.get(race_string, headers={'Authorization': '{0}' '{1}'.format(
                    'Accept', 'application/json'), 'Content-Type': 'application/json'})
                race.raise_for_status()  # generate status code to see if successful request recieved
                try:
                    t = race.text  # generate text from response object
                    race_j = json.loads(t)  # load to python dict obj
                except ValueError as e:
                    print("Empty JSON response.")
                    logging.warning(e)
                    break

            except race.exceptions.HTTPError as http_err:
                print(
                    "Status code exceeded acceptable range: 200 <= x < 300 [Status code", race.status_code, "]")
                logging.warning(http_err)

            except ConnectionError as conn_err:
                logging.warning(conn_err)

            else:

                if not race_j["MRData"]:  # dict is empty
                    logging.critical("Empty JSON")

                else:  # dict is not empty

                    try:  # get the name of the race to indicate a valid response

                        # check for empty json response object for race key
                        assert race_j['MRData']['RaceTable']['Races'] != [
                        ], "Race number exceeded season boundary. Data now up to date."

                        for race in race_j['MRData']['RaceTable']['Races']:

                            # get race date from json, change to datetime object and compare to extract_dt
                            race_date_str = race['date']
                            # formatting into datetime obj
                            race_date = datetime.strptime(
                                race_date_str, dt_format)

                            # if after date then initiate download of data.
                            if race_date > extract_dt:

                                race_season = race['season']
                                race_name = race['raceName']
                                track_name = race['Circuit']['circuitName']
                   
                                for results in race['Results']:

                                    driver_name = results['Driver']['familyName']
                                    driver_no = results['Driver']['permanentNumber']
                                    driver_identifier = results['Driver']['code']
                                    finishing_pos = results['position']
                                    team_name = results['Constructor']['constructorId']
                                    start_pos = results['grid']
                                    points = results['points']
                                    race_status = results['status']

                                    # try and get the fastest lap for each driver
                                    try:
                                        fastest_lap = results['FastestLap']['Time']['time']
                                    except KeyError as err:
                                        # set the fastest_lap to 0
                                        debug_print(results, str(err))
                                        logging.info(
                                            f"No laptime available for {driver_name} - setting fastest lap to 0.0.")
                                        fastest_lap = 0.0

                                    race_n = {"race_id": race_name, "track_name": track_name, "quali_date": race['date'], "season_year": race_season,
                                              "driver_name": driver_name, "driver_no": driver_no, "driver_identifier": driver_identifier,
                                              "team_name": team_name, "start_pos": start_pos, "finishing_pos": finishing_pos, "fastest_lap": fastest_lap,
                                              "points": points, "race_status": race_status}

                                    # adding quali result data to list
                                    race_list.append(race_n)

                            else:
                                logging.info(
                                    "Data exists already - race date precedes extract date. Skipping.")

                            round_no += 1  # increment counter

                    except AssertionError as e:
                        logging.info(e)
                        break

        return race_list

    def incremental_quali_telem(self, cache_dir, start_date, end_date):
        """ Extracts qualifying telemetry data from fastf1 api, for the incremental load pathway. 
        Args: 
        ti : type=task_instance - responsible for accessing the current running task instance for xcoms data
        cache_dir = textfile - used to access the os directory where cache is stored or fast downloads."""

        import pandas as pd
        import fastf1 as f1
        import numpy as np
        import logging
        import traceback

        # qualifying telemetry

        f1.Cache.enable_cache(cache_dir)
        ultimate = []  # empty list to store dataframe for all results across multiple seasons
        quali_telem_table = pd.DataFrame()

        # outer for loop for seasons
        for s in range(start_date, end_date):
            size = f1.get_event_schedule(s)
            # get the number of races in the season by getting all race rounds greater than 0
            race_no = size.query("RoundNumber > 0").shape[0]
            race_name_list = []  # empty list for storing races
            final = []  # empty list for aggregated race data for all drivers during season

            # get all the names of the races for this season
            for i in range(1, race_no):

                event = size.get_event_by_round(i)
                # access by column to get value of race
                race_name = event.loc["EventName"]
                race_name_list.append(race_name)
                session = f1.get_session(
                    s, i, identifier='Q', force_ergast=False)

                try:

                    # load all driver information for session
                    session.load(telemetry=True, laps=True, weather=True)

                    # load weather data
                    weather_data = session.laps.get_weather_data()

                    assert type(
                        weather_data) == pd.core.frame.DataFrame, "Weather data unavailable for this race weekend."
                except f1.core.DataNotLoadedError:
                    logging.error(
                        "Telemetry data unavailable for Race Weekend No.{0}.".format(i))
                    continue
                except AssertionError as msg:
                    logging.error(msg)
                    continue

                # selecting columns to be dropped from weather df
                col = ["Time", "AirTemp", "Pressure", "WindDirection"]
                for c in col:
                    weather_data.drop(c, axis=1, inplace=True)
                    weather_data = weather_data.reset_index(drop=True)

                drivers = session.drivers
                listd = []
                series = []
                driver_list = []

                # loop through every driver in the race
                for d in drivers:

                    # load all race telemetry session for driver
                    driver_t = session.laps.pick_driver(d)
                    driver_telem = driver_t.reset_index(drop=True)
                    listd.append(driver_telem)  # append information to list
                    drive = pd.concat(listd)  # concat to pandas series
                    # add the season the telemetry pertains to
                    drive["season_year"] = s
                    # add the race the telemetry pertains to
                    drive["race_name"] = race_name

                    # Additional info
                    driver_info = session.get_driver(d)
                    name = driver_info["FullName"]
                    drive["driver_name"] = name

                    # invert columns and values
                    driver_df = pd.DataFrame(drive)

                    # telemetry data for drivers
                    try:

                        telemetry = session.laps.pick_driver(d).get_telemetry()
                        columns = ["Time", "DriverAhead", "SessionTime", "Date", "DRS", "Source",
                                   "Distance", "RelativeDistance", "Status", "X", "Y", "Z", "Brake"]
                        for c in columns:  # dropping irrelevant columns
                            telemetry.drop(c, axis=1, inplace=True)
                        driver_telem = telemetry.reset_index(
                            drop=True)  # dropping index
                        # creating dataframe from dict object
                        dt_quali = pd.DataFrame.from_dict(driver_telem)
                        series.append(dt_quali)  # appending to list
                        # concatenating to dataframe
                        driver_t = pd.concat(series)

                        driver_telem_df = pd.DataFrame(driver_t)

                        # append weather data, and telemetry data to existing dataframe of lap data
                        telem = pd.concat(
                            [driver_df, weather_data, driver_telem_df], ignore_index=True, sort=False)
                        driver_list.append(telem)
                    except ValueError:
                        logging.warning(
                            "No telemetry data available for Driver: {0} - No.{1}".format(name, d))
                        continue

                try:
                    # drivers in a race weekend
                    drivers_data = pd.concat(driver_list)
                    final.append(drivers_data)
                except ValueError:
                    logging.error(
                        "Telemetry data unavailable for Race Weekend No.{0}".format(i))
                    continue
            try:
                # all races in a season
                processed_data = pd.concat(final)
                ultimate.append(processed_data)

            except ValueError:
                logging.error(
                    "Telemetry data unavailable for the {0} season.".format(s))
                continue

        column = ["SpeedST", "Driver", "IsPersonalBest", "PitOutTime", "PitInTime", "Sector1SessionTime", "Sector2SessionTime", "Sector3SessionTime",
                  "Time", "WindSpeed",  "SpeedI1", "SpeedI2", "SpeedFL", "DistanceToDriverAhead", "LapStartDate", "LapStartTime", "TrackStatus"]

        try:
            # concatenate data across seasons for full load
            quali_telem_table = pd.concat(ultimate)

            # drop irrelevant columns in dataframe
            for c in column:
                quali_telem_table.drop(c, axis=1, inplace=True)

            # convert time deltas to strings and reformat
            col = ["LapTime", "Sector1Time", "Sector2Time", "Sector3Time"]

            for c in col:
                quali_telem_table[c] = quali_telem_table[c].astype(
                    str).map(lambda x: x[10:])

            # replace all empty values with 0
            quali_telem_table.replace(
                r'^\s*$', 0, regex=True, inplace=True)

            # provide desired column names to dataframe
            quali_telem_table.columns = ["car_no", "lap_time", "lap_no", "s1_time", "s2_time", "s3_time", "compound", "tyre_life", "fresh_set", "race_stint", "team_name",
                                         "IsAccurate", "season_year", "race_name", "driver_name", "humidity", "occur_of_rain_quali", "track_temp", "revs_per_min", "car_speed", "gear_no", "throttle_pressure"]

        except ValueError:
            logging.error("Exception desc: {0}".format(e))
            logging.error("Unable to concatenate. DataFrame is empty.")
            traceback.print_exc()
        except KeyError as e:
            logging.error(
                "Key given in array not present in dataframe column.")
            traceback.print_exc()

        return quali_telem_table

    def incremental_race_telem(self, ti, cache_dir):
        """ Extracts race telemetry data from fastf1 api, for the incremental load pathway. 
        Args: 
        ti : type=task_instance - responsible for accessing the current running task instance for xcoms data
        cache_dir = textfile - used to access the os directory where cache is stored or fast downloads."""

        import logging
        from decouple import config
        import fastf1 as f1
        import pandas as pd
        import numpy as np

        # setting parameters
        race_name = []
        dt_format = self._dt_format
        path = config("pathway")
        msg = "Data unavailable due to lack of api support. Dataframe is empty for this season."

        cache_dp = cache_dir.format(extract_dt)

        f1.Cache.enable_cache(cache_dp)
        ultimate = []  # empty list to store dataframe for all results across multiple seasons

        # initialising variables
        size = f1.get_event_schedule(season)
        # get the number of races in the season by getting all race rounds greater than 0
        race_no = size.query("RoundNumber > 0").shape[0]
        race_name_list = []  # empty list for storing races
        final = []  # empty list for aggregated race data for all drivers during season
        race_number_list = []  # empty list for storing the index of valid races

        # setting extract parameters
        if self.validate_pathway_file(path) == 'Full':
            extract_dt = ti.xcom_pull(
                task_ids='full_load_serialization', key='extract_date')
        elif self.validate_pathway_file(path) == 'Incremental':
            extract_dt = ti.xcom_pull(
                task_ids='inc_load_serialization', key='incremental_extract_date')

        # getting season year variable from extract_dt param
        season = self.extract_year(extract_dt, dt_format)

        # get all the names of the races for this season
        for i in range(1, race_no):

            event = size.get_event_by_round(i)
            # access by column to get value of race
            race_name = event.loc["EventName"]
            # access by column to get value of date
            race_date = event.loc["EventDate"]

            if race_date > extract_dt:  # condition for incremental download of new data

                # appending to array the race index
                race_number_list.append(i)
                # appending information to array
                race_name_list.append(race_name)

            else:

                logging.info(
                    "Race date precedes extract date. Skipping download.")
                continue

        # THEN PUT THESE INDEXES IN ANOTHER ARRAY AND LOOP THROUGH USING THEM AS THE RACES TO DOWNLOAD

        items = len(race_number_list)
        for i in range(1, items):
            number = race_name_list[i]

            session = f1.get_session(
                season, number, identifier='R', force_ergast=False)

            try:
                # load all driver information for session
                session.load(telemetry=True, laps=True,
                             weather=True), "Data unavailable for this session, skipping entirely."
                # load weather data
                weather_data = session.laps.get_weather_data()

            except f1._api.SessionNotAvailableError as msg:
                logging.error(msg)
                continue
            except f1.core.DataNotLoadedError as msg:
                logging.error(msg)
                continue

            # selecting columns to be dropped from weather df
            col = ["Time", "AirTemp", "Pressure", "WindDirection"]
            for c in col:
                weather_data.drop(c, axis=1, inplace=True)
                weather_data = weather_data.reset_index(drop=True)

                drivers = session.drivers
                listd = []
                series = []
                driver_list = []

                # loop through every driver in the race
                for d in drivers:
                    # load all race telemetry session for driver
                    driver_t = session.laps.pick_driver(d)
                    driver_telem = driver_t.reset_index(drop=True)
                    listd.append(driver_telem)  # append information to list
                    drive = pd.concat(listd)  # concat to pandas series
                    # add the season the telemetry pertains to
                    drive["season_year"] = season
                    # add the race the telemetry pertains to
                    drive["race_name"] = race_name
                    drive["fastest_lap"] = session.laps.pick_driver(
                        d).pick_fastest()

                    # telemetry data for drivers
                    try:
                        telemetry = session.laps.pick_driver(d).get_telemetry()
                        columns = ["Time", "DriverAhead", "SessionTime", "Date", "DRS", "Source",
                                   "Distance", "RelativeDistance", "Status", "X", "Y", "Z", "Brake"]
                        logging.info("Dropping columns...")
                        for c in columns:  # dropping irrelevant columns
                            telemetry.drop(c, axis=1, inplace=True)
                      
                        driver_telem = telemetry.reset_index(
                            drop=True)  # dropping index
                        # creating dataframe from dict object
                        dt_quali = pd.DataFrame.from_dict(driver_telem)
                        series.append(dt_quali)  # appending to list
                        # concatenating to dataframe
                        driver_t = pd.concat(series)

                        # append weather data, and telemetry data to existing dataframe of lap data
                        telem = pd.concat(
                            [drive, weather_data, driver_t], ignore_index=True, sort=False)
                        driver_list.append(telem)
                    except ValueError:
                        logging.warning(
                            "No telemetry data available for car number - {}".format(d))
                        continue
                try:
                    # drivers in a race
                    drivers_data = pd.concat(driver_list)
                    final.append(drivers_data)
                except ValueError as msg:
                    logging.warning(msg)
                    continue
            try:
                # all races in a season
                processed_data = pd.concat(final)
                ultimate.append(processed_data)
            except ValueError as msg:
                logging.warning(msg)
                continue
        try:
            race_telemetry_table = pd.concat(ultimate)

        except ValueError as msg:
            logging.warning(msg)

        column = ["TrackStatus", "LapStartTime", "LapStartDate", "Sector1SessionTime", "FreshTyre", "Time",
                  "Sector2SessionTime", "Sector3SessionTime", "SpeedI1", "SpeedI2", "WindSpeed", "SpeedFL", "SpeedST"]
        logging.info("Dropping columns...")
        # drop irrelevant columns
        for c in column:
            race_telemetry_table.drop(c, axis=1, inplace=True)

        # convert time deltas to strings and reformat
        col = ["LapTime", "Sector1Time", "Sector2Time",
               "Sector3Time", "fastest_lap"]
        for c in col:
            race_telemetry_table[c] = race_telemetry_table[c].astype(
                str).map(lambda x: x[10:])

        # replace all empty values with 0
        race_telemetry_table.replace(
            r'^\s*$', 0, regex=True, inplace=True)

        # provide desired column names
        race_telemetry_table.columns = ["car_no", "lap_time", "lap_no", "time_out", "time_in", "s1_time", "s2_time", "s3_time", "IsPersonalBest", "compound", "tyre_life", "race_stint", "team_name",
                                        "driver_identifier", "IsAccurate", "season_year", "race_name", "fastest_lap", "pit_duration", "humidity", "occur_of_rain_race", "track_temp",  "revs_per_min", "car_speed", "gear_no", "throttle_pressure"]

        return race_telemetry_table

    def extract_quali_telem(self, cache_dir, start_date, end_date) -> pd.DataFrame:
        """ Full load of telemetry data for qualifying for each event of the race calendar.
            Takes the cache directory, start and end date for data load as parameters, and returns a dataframe."""

        import logging
        import pandas as pd
        import fastf1 as f1
        import numpy as np

        # enabling cache for faster data retrieval
        f1.Cache.enable_cache(cache_dir)
        ultimate = []  # empty list to store dataframe for all results across multiple seasons

        # outer for loop for seasons
        for s in range(start_date, end_date):
            size = f1.get_event_schedule(s)
            # get the number of races in the season by getting all race rounds greater than 0
            race_no = size.query("RoundNumber > 0").shape[0]
            race_name_list = []  # empty list for storing races
            final = []  # empty list for aggregated race data for all drivers during season

            # get all the names of the races for this season
            for i in range(1, race_no):

                event = size.get_event_by_round(i)
                # access by column to get value of race
                race_name = event.loc["EventName"]
                race_name_list.append(race_name)
                session = f1.get_session(
                    s, i, identifier='Q', force_ergast=False)

                try:
                    # load all driver information for session
                    session.load(
                        telemetry=True, laps=True, weather=True), "Data unavailable for this session, skipping entirely."
                    # load weather data
                    weather_data = session.laps.get_weather_data()
                    assert type(
                        weather_data) == pd.core.frame.DataFrame, "[ERROR] Weather data unavailable for this race weekend."

                except f1._api.SessionNotAvailableError:
                    logging.error(
                        "[ERROR] Telemetry data unavailable for Race Weekend No.{0}.".format(i))
                    continue
                except f1.core.DataNotLoadedError:
                    logging.error(
                        "[ERROR] Telemetry data unavailable for Race Weekend No.{0}.".format(i))
                    continue
                except AssertionError as msg:
                    logging.error(msg)
                    continue

                # selecting columns to be dropped from weather df
                col = ["Time", "AirTemp", "Pressure", "WindDirection"]
                logging.info("Dropping weather columns...")
                for c in col:
                    weather_data.drop(c, axis=1, inplace=True)
                    weather_data = weather_data.reset_index(drop=True)

                drivers = session.drivers  # list of drivers in the session
                listd = []
                series = []
                driver_list = []

                # loop through every driver in the race
                for d in drivers:

                    # load all race telemetry session for driver
                    driver_t = session.laps.pick_driver(d)
                    driver_telem = driver_t.reset_index(drop=True)
                    listd.append(driver_telem)  # append information to list
                    drive = pd.concat(listd)  # concat to pandas series
                    # add the season the telemetry pertains to
                    drive["season_year"] = s
                    # add the race the telemetry pertains to
                    drive["race_name"] = race_name
                    drive["pole_lap"] = session.laps.pick_fastest()
                    driver_info = session.get_driver(d)
                    name = driver_info["FullName"]

                    # telemetry data for drivers
                    try:
                        telemetry = session.laps.pick_driver(d).get_telemetry()
                        columns = ["Time", "DriverAhead", "SessionTime", "Date", "DRS", "Source",
                                   "Distance", "RelativeDistance", "Status", "X", "Y", "Z", "Brake"]
                        logging.info("Dropping driver telemetry columns...")
                        for c in columns:  # dropping irrelevant columns
                            telemetry.drop(c, axis=1, inplace=True)
                

                        driver_telem = telemetry.reset_index(
                            drop=True)  # dropping index
                        # creating dataframe from dict object
                        dt_quali = pd.DataFrame.from_dict(driver_telem)
                        series.append(dt_quali)  # appending to list
                        # concatenating to dataframe
                        driver_t = pd.concat(series)

                        # append weather data, and telemetry data to existing dataframe of lap data
                        telem = pd.concat(
                            [drive, weather_data, driver_t], ignore_index=True, sort=False)
                        driver_list.append(telem)
                    except ValueError:
                        logging.warning(
                            "[ALERT] No telemetry data available for Driver: {0} - No.{1}".format(name, d))
                        continue

                try:
                    # drivers in a race
                    drivers_data = pd.concat(driver_list)
                    final.append(drivers_data)
                except ValueError:
                    logging.error(
                        "[ERROR] Telemetry data unavailable for Race Weekend No.{0}".format(i))
                    continue

            try:
                # all races in a season
                processed_data = pd.concat(final)
                ultimate.append(processed_data)

            except ValueError:
                logging.error(
                    "[ERROR] Telemetry data unavailable for the {0} season.".format(s))
                continue

            quali_telemetry_table = pd.concat(ultimate)

            columns = ["SpeedST", "IsPersonalBest", "PitOutTime", "PitInTime", "TrackStatus", "LapStartTime", "LapStartDate", "Sector1SessionTime",
                       "FreshTyre", "Time", "Sector2SessionTime", "Sector3SessionTime", "SpeedI1", "SpeedI2", "WindSpeed", "SpeedFL", "DistanceToDriverAhead"]
            # drop irrelevant columns
            for c in columns:
                quali_telemetry_table.drop(c, axis=1, inplace=True)

            # convert time deltas to strings and reformat
            col = ["LapTime", "Sector1Time",
                   "Sector2Time", "Sector3Time", "pole_lap"]
            for c in col:
                quali_telemetry_table[c] = quali_telemetry_table[c].astype(
                    str).map(lambda x: x[10:])

            # replace all empty values with 0
            quali_telemetry_table.replace(
                r'^\s*$', 0, regex=True, inplace=True)

            # provide desired column names
            quali_telemetry_table.columns = ["car_no", "lap_time", "lap_no", "s1_time", "s2_time", "s3_time", "compound", "tyre_life", "race_stint", "team_name", "driver_identifier",
                                             "IsAccurate", "season_year", "race_name", "pole_lap", "humidity", "occur_of_rain_quali", "track_temp", "revs_per_min", "car_speed", "gear_no", "throttle_pressure"]

        return quali_telemetry_table

    def extract_race_telem(self, cache_dir, start_date, end_date) -> pd.DataFrame:
        """Full load of telemetry data for qualifying for each event of the race calendar.
            Takes the cache directory, start and end date for data load as parameters, and returns a dataframe."""

        import logging
        import pandas as pd
        import fastf1 as f1
        import numpy as np

        # enabling cache for faster data retrieval
        f1.Cache.enable_cache(cache_dir)
        ultimate = []  # empty list to store dataframe for all results across multiple seasons
        msg = "Telemetry data unavailable for this season, dataframe is empty."

        # outer for loop for seasons
        for s in range(start_date, end_date):
            size = f1.get_event_schedule(s)
            # get the number of races in the season by getting all race rounds greater than 0
            race_no = size.query("RoundNumber > 0").shape[0]
            race_name_list = []  # empty list for storing races
            final = []  # empty list for aggregated race data for all drivers during season

            # get all the names of the races for this season
            for i in range(1, race_no):

                event = size.get_event_by_round(i)
                # access by column to get value of race
                race_name = event.loc["EventName"]
                race_name_list.append(race_name)
                session = f1.get_session(s, i, 'R', force_ergast=False)

                try:
                    # load all driver information for session
                    session.load(telemetry=True, laps=True, weather=True)

                except f1.core.DataNotLoadedError:
                    logging.error(
                        "Telemetry data unavailable for Race Weekend No.{0}.".format(i))
                    continue
                except AssertionError as msg:
                    logging.error(msg)
                    continue

                try:
                    # load weather data
                    weather_data = session.laps.get_weather_data()

                    assert type(
                        weather_data) == pd.core.frame.DataFrame, "Weather data unavailable for this race weekend."
                except AssertionError as e:
                    logging.error(e)
                    continue
                except f1.core.DataNotLoadedError:
                    logging.error(
                        "Telemetry data unavailable for Race No.{0}".format(i))
                    continue

                # selecting columns to be dropped from weather df
                col = ["Time", "AirTemp", "Pressure", "WindDirection"]
                logging.info("Dropping columns...")
                for c in col:
                    weather_data.drop(c, axis=1, inplace=True)
                    weather_data = weather_data.reset_index(drop=True)

                drivers = session.drivers
                listd = []
                series = []
                driver_list = []

                # loop through every driver in the race
                for d in drivers:
                    # load all race telemetry session for driver
                    driver_t = session.laps.pick_driver(d)
                    driver_telem = driver_t.reset_index(drop=True)
                    listd.append(driver_telem)  # append information to list
                    drive = pd.concat(listd)  # concat to pandas series
                    # add the season the telemetry pertains to
                    drive["season_year"] = s
                    # add the race the telemetry pertains to
                    drive["race_name"] = race_name
                    drive["fastest_lap"] = session.laps.pick_driver(
                        d).pick_fastest()

                    # Additional info
                    driver_info = session.get_driver(d)
                    name = driver_info["FullName"]
                    drive["driver_name"] = name

                    # invert columns and values
                    driver_df = pd.DataFrame(drive)

                    # telemetry data for drivers
                    try:
                        telemetry = session.laps.pick_driver(d).get_telemetry()
                        columns = ["Time", "DriverAhead", "SessionTime", "Date", "DRS", "Source",
                                   "Distance", "RelativeDistance", "Status", "X", "Y", "Z", "Brake"]
                        logging.info("Dropping telemetry columns...")
                        for c in columns:  # dropping irrelevant columns
                            telemetry.drop(c, axis=1, inplace=True)

                        driver_telem = telemetry.reset_index(
                            drop=True)  # dropping index
                        # creating dataframe from dict object
                        dt_race = pd.DataFrame.from_dict(driver_telem)
                        series.append(dt_race)  # appending to list
                        # concatenating to dataframe
                        driver_t = pd.concat(series)

                        driver_telem_df = pd.DataFrame(driver_t)

                        # append weather data, and telemetry data to existing dataframe of lap data
                        telem = pd.concat(
                            [driver_df, weather_data, driver_telem_df], ignore_index=True, sort=False)
                        driver_list.append(telem)
                    except ValueError:
                        logging.warning(
                            "No telemetry data available for Driver: {0} - No.{1}".format(name, d))
                        continue
                    except f1.core.DataNotLoadedError as e:
                        logging.error(e)
                        continue
                try:
                    # drivers in a race weekend
                    drivers_data = pd.concat(driver_list)
                    final.append(drivers_data)
                except ValueError:
                    logging.error(
                        "Telemetry data unavailable for Race Weekend No.{0}".format(i))
                    continue
            try:
                # all races in a season
                processed_data = pd.concat(final)
                ultimate.append(processed_data)
            except ValueError:
                logging.error(
                    "Telemetry data unavailable for the {0} season.".format(s))
                continue

        try:
            race_telemetry_table = pd.concat(ultimate)

        except ValueError as msg:
            logging.warning(msg)

        column = ["Time", "TrackStatus", "LapStartTime",
                  "LapStartDate", "Sector1SessionTime", "FreshTyre", "Sector2SessionTime",
                  "Sector3SessionTime", "SpeedI1", "SpeedI2", "WindSpeed", "SpeedFL", "SpeedST",
                  "Driver"]

        try:
            # concatenate data across seasons for full load
            race_telem_table = pd.concat(ultimate)

            logging.info("Dropping race telemetry columns...")

            # drop irrelevant columns
            for c in column:
                race_telem_table.drop(c, axis=1, inplace=True)

            # convert time deltas to strings and reformat
            col = ["LapTime", "Sector1Time", "Sector2Time",
                   "Sector3Time", "fastest_lap"]
            for c in col:
                race_telem_table[c] = race_telem_table[c].astype(
                    str).map(lambda x: x[10:])

            # replace all empty values with 0
            race_telem_table.replace(
                r'^\s*$', 0, regex=True, inplace=True)

            # provide desired column names
            race_telem_table.columns = ["car_no", "lap_time", "lap_no", "time_out", "time_in", "s1_time", "s2_time", "s3_time", "IsPersonalBest", "compound", "tyre_life", "race_stint", "team_name",
                                        "driver_identifier", "IsAccurate", "season_year", "race_name", "fastest_lap", "pit_duration", "humidity", "occur_of_rain_race", "track_temp", "revs_per_min", "car_speed", "gear_no", "throttle_pressure"]

        except ValueError as e:
            logging.error("Exception desc: {0}".format(e))
            logging.error("Unable to concatenate. DataFrame is empty.")
        except KeyError:
            logging.error(
                "Key given in array not present in dataframe column.")

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
        
    def path_generator(self, load_type, home_dir, network_scope, pathway):
        pass
    
    def cache_cleaner(self, load_type, home_dir, network_scope, pathway):
        pass
    
    def is_not_empty(self, s):
        pass

    def csv_producer(self, user_home, extract_dt, dataframes):
        pass

    def determine_format(self):
        pass

    def extract_year(self, date_obj, dt_format):
        pass

    def validate_pathway_file(self, file):
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
