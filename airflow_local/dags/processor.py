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
    def csv_producer(self, user_home, extract_dt, dataframes):
        pass

    @abstractmethod
    def serialize_full(self, **arg) -> datetime:
        pass

    @abstractmethod
    def increment_serialize(self, qualifying_table, results_table, race_telem_table, quali_telem_table, ti) -> datetime:
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
        import os
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

    def postgres_transformations(self):
        pass

    def extract_season_grain(self, cache_dir, start_date, end_date) -> pd.DataFrame:
        """ Extracts the season grain data from the api and aggregates them together ready for loading into the database.
            takes a target cache directory as the input for first variable,
            start date and end date for extraction of f1 data as the next two. """

        import logging
        import fastf1 as f1
        import pandas as pd

        logging.info(
            "------------------------------Extracting Aggregated Season Data--------------------------------------")

        # enable cache for faster data retrieval
        f1.Cache.enable_cache(cache_dir)

        data_holder = []
        # outer for loop for seasons
        for s in range(start_date, end_date):
            size = f1.get_event_schedule(s)
            size["season_year"] = s
            data_holder.append(size)

        # CREATE data frame for season data
        for d in data_holder:

            season = pd.concat(data_holder)

        # drop session 1,2,3, and dates, f1api support, officialeventname from season data
        logging.info("Dropping columns...")
        columns = ["OfficialEventName", "Session1", "Session1Date", "Session2", "Session2Date", "Session3", "Session3Date", "Session4", "Session5",
                   "F1ApiSupport", "EventFormat", "EventDate", "Session1DateUtc", "Session2DateUtc", "Session3DateUtc", "Session4DateUtc", "Session5DateUtc"]
        season.drop(columns, axis=1, inplace=True)

        # rename columns
        season.columns = ["race_round", "country", "city",
                          "race_name", "quali_date", "races_date", "season_year"]
        season.index += 1
        season.index.name = "race_id"

        # splitting the date and time apart
        try:
            season["race_date"] = pd.to_datetime(season['races_date']).dt.date
            season["race_time"] = pd.to_datetime(season["races_date"]).dt.time
            season["quali_date"] = pd.tp_datetime(season["quali_date"]).dt.date

        except ValueError:

            season["race_date"] = 0
            season["race_time"] = 0
            season["quali_date"] = 0

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

        logging.info(
            "------------------------------Extracting Aggregated Qualifying Data--------------------------------------")

        # enable cache for faster data retrieval
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

                jam = f1.get_event(s, i)
                # access by column to get value of race
                race_name = jam.loc["EventName"]
                race_name_list.append(race_name)

                ff1 = f1.get_session(s, i, 'Q')
                # load all driver information for session
                ff1.load()
                drivers = ff1.drivers
                listd = []
                # loop through every driver in the race
                for d in drivers:

                    name = ff1.get_driver(d)
                    newname = name.to_frame().T  # invert columns and values

                    columns = ["BroadcastName", "Time", "FullName", "Status", "Points", "GridPosition",
                               "HeadshotUrl", "CountryCode", "ClassifiedPosition", "Abbreviation", "TeamId"]

                    logging.info("Dropping columns...")
                    # drop irrelevant columns
                    for c in columns:
                        newname.drop(c, axis=1, inplace=True)

                    # provide desired column names
                    newname.columns = ["car_number", "driver_id", "team_name", "team_colour",
                                       "forename", "surname", "quali_pos", "best_q1", "best_q2", "best_q3"]
                    # provide new index
                    newname.index.name = "driver_id"

                    newname["season_year"] = s
                    newname["race_name"] = race_name
                    newname
                    # convert time deltas to strings and reformat
                    col = ["best_q1", "best_q2", "best_q3"]
                    for c in col:
                        newname[c] = newname[c].astype(
                            str).map(lambda x: x[10:])

                    # replace all empty values with NaN
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
        import logging

        # enable cache for faster data retrieval
        f1.Cache.enable_cache(cache_dir)
        ultimate = []  # empty list to store dataframe for all results across multiple seasons

        # outer for loop for seasons
        for s in range(start_date, end_date):
            size = f1.get_event_schedule(s)

            # get the number of races in the season by getting all race rounds greater than 0
            race_no = size.query("RoundNumber > 0").shape[0]

            if s == end_date:
                incremental_race_counter = race_no

            race_name_list = []  # empty list for storing races
            final = []  # empty list for aggregated race data for all drivers during season

            # get all the names of the races for this season
            for i in range(1, race_no):

                jam = f1.get_event(s, i)
                # access by column to get value of race
                race_name = jam.loc["EventName"]
                print(race_name)
                race_name_list.append(race_name)

                ff1 = f1.get_session(s, i, 'R')
                # load all driver information for session
                ff1.load()
                drivers = ff1.drivers
                listd = []

                # loop through every driver in the race
                for d in drivers:

                    name = ff1.get_driver(d)
                    newname = name.to_frame().T  # invert columns and values

                    columns = ["BroadcastName", "FullName", "Q1", "Q2", "Q3", "Abbreviation",
                               "CountryCode", "TeamId", "TeamColor", "HeadshotUrl", "ClassifiedPosition"]

                    logging.info("Dropping columns...")
                    # drop irrelevant columns
                    for c in columns:
                        newname.drop(c, axis=1, inplace=True)

                    # provide desired column names
                    newname.columns = ["car_number", "driver_id", "team_name", "forename", "surname",
                                       "finish_pos", "start_pos", "Time", "driver_status_update", "race_points"]
                    # provide new index
                    newname.index.name = "driver_id"

                    newname["pos_change"] = newname["start_pos"] - \
                        newname["finish_pos"]
                    newname["season_year"] = s
                    newname["race_name"] = race_name
                    newname
                    # convert time deltas to strings and reformat
                    col = ["Time"]

                    for c in col:
                        newname[c] = newname[c].astype(
                            str).map(lambda x: x[10:])

                    # replace all empty values with NaN
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

    def csv_producer(self, user_home, extract_dt, dataframes):
        """ """

        i = 0  # counter variable

        # append date to the end of the files, and output to the correct directory
        for d in dataframes:

            i += 1  # increment counter

            if i == 1:
                d.to_csv('{}/cache/FullProcessed/Results{}.csv'.format(user_home,
                         extract_dt), index=False, header=True)

            elif i == 2:
                d.to_csv('{}/cache/FullProcessed/Qualifying{}.csv'.format(user_home,
                         extract_dt), index=False, header=True)

            elif i == 3:
                d.to_csv('{}/cache/FullProcessed/Season{}.csv'.format(user_home,
                         extract_dt), index=False, header=True)

            elif i == 4:
                d.to_csv('{}/cache/FullProcessed/RaceTelem{}.csv'.format(user_home,
                         extract_dt), index=False, header=True)

            elif i == 5:
                d.to_csv('{}/cache/FullProcessed/QualifyingTelem{}.csv'.format(
                    user_home, extract_dt), index=False, header=True)

        return

    def serialize_full(self, **arg):
        """ This method is responsible for serializing the files into csv format before they are uploaded in raw form to an s3 bucket for persistence."""
        import logging
        from datetime import datetime
        from decouple import config

        logging.info(
            "------------------------------Serializing DataFrames to CSV--------------------------------------")

        extract_dt = datetime.today().strftime(
            "%Y-%m-%d")  # date stamp for record keeping

        # directory which the processed csv files are output to as a result of this function
        processed_dir = config("user_home")

        if arg[3] == 1:  # full load

            dataframes = [arg[0], arg[1], arg[2]]

        elif arg[3] == 2:  # full telemetry load

            dataframes = [arg[4], arg[5]]

        # converting dataframe to csv and storing in specified location
        self.csv_producer(processed_dir, extract_dt, dataframes)

        return extract_dt  # return the date to push to xcoms for s3 hook to identify the file by the date for uploading into postgres

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
        race_name = []
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
                    race_j = json.loads(t)  # load to python dict obj
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

                if not race_j["MRData"]:  # dict is empty
                    logging.critical("Empty JSON")

                else:  # dict is not empty

                    try:  # get the name of the race to indicate a valid response

                        # check for empty json response object for race key
                        assert race_j['MRData']['RaceTable']['Races'] != [
                        ], "Race number exceeded season boundary. Data now up to date."

                        # loop through race array
                        for race in race_j['MRData']['RaceTable']['Races']:

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
                                    race_name.append(race_n)

                            # get race date from json, change to datetime object and compare to extract_dt
                            # if after date then initiate download of data.

                            else:
                                logging.info(
                                    "Data exists already - race date precedes extract date. Skipping.")

                            round_no += 1  # increment counter for while loop

                    except AssertionError as e:
                        logging.info(e)
                        break

        return race_name

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
                task_ids='full_load_serialization', key='extract_date')
        elif self.validate_pathway_file(path) == 'Incremental':
            extract_dt = ti.xcom_pull(
                task_ids='inc_load_serialization', key='incremental_extract_date')

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
                                            "Setting fastest lap as 0 for driver.")
                                        fastest_lap = 0

                                    race_n = {"race_id": race_name, "quali_date": race['date'], "season_year": race_season,
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

            # replace all empty values with NaN
            quali_telem_table.replace(
                r'^\s*$', np.nan, regex=True, inplace=True)

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
                        print(telemetry.columns)
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

        # replace all empty values with NaN
        race_telemetry_table.replace(
            r'^\s*$', np.nan, regex=True, inplace=True)

        # provide desired column names
        race_telemetry_table.columns = ["car_no", "lap_time", "lap_no", "time_out", "time_in", "s1_time", "s2_time", "s3_time", "IsPersonalBest", "compound", "tyre_life", "race_stint", "team_name",
                                        "driver_identifier", "IsAccurate", "season_year", "race_name", "fastest_lap", "pit_duration", "humidity", "occur_of_rain_race", "track_temp",  "revs_per_min", "car_speed", "gear_no", "throttle_pressure"]

        return race_telemetry_table

    def increment_serialize(self, qualifying_table, results_table, race_telem_table, quali_telem_table):
        """ """

        import logging
        from datetime import datetime
        from decouple import config
        import pandas as pd

        logging.info(
            "------------------------------Serializing DataFrames to CSV--------------------------------------")

        extract_dt = datetime.today().strftime("%Y-%m-%d")

        dataframes = [qualifying_table, results_table]
        arrays = [race_telem_table, quali_telem_table]
        user_home = config("user_home")

        i = 0  # counter variable
        j = 0
        # append date to the end of the files, and output to the correct directory
        for d in dataframes:

            i += 1  # increment counter

            if i == 1:
                d.to_csv('Users/{}/cache/IncrementalProcessed/Qualifying{}.csv'.format(
                    user_home, extract_dt), index=False, header=True)

            elif i == 2:
                d.to_csv('Users/{}/cache/IncrementalProcessed/Results{}.csv'.format(
                    user_home, extract_dt), index=False, header=True)

        for a in arrays:  # need to change this to handle string arrays and convert them to csv
            j += 1
            if j == 1:
                df = pd.DataFrame(a)
                df.to_csv('/Users/{}/cache/IncrementalProcessed/RaceTelem{}.csv'.format(
                    user_home, extract_dt), index=False, header=True)

            elif j == 2:
                df = pd.DatFrame(a)
                df.to_csv('/Users/{}/cache/IncrementalProcessed/QualifyingTelem{}.csv'.format(
                    user_home, extract_dt), index=False, header=True)

        return extract_dt

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
                        print(telemetry.columns)

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

            # replace all empty values with NaN
            quali_telemetry_table.replace(
                r'^\s*$', np.nan, regex=True, inplace=True)

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

            # replace all empty values with NaN
            race_telem_table.replace(
                r'^\s*$', np.nan, regex=True, inplace=True)

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

    def serialize_full(self, user_home, race_table, driver_table, season_table, race_telem_table, quali_telem_table) -> datetime:
        pass

    def increment_serialize(self, qualifying_table, results_table, race_telem_table, quali_telem_table, ti) -> datetime:
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
