import fastf1
import pandas as pd
import os
from datetime import datetime, timedelta, timezone
import argparse
import json
from loguru import logger
from helper_functions import *

logger.add("file.log")

logger.info("Starting....")

parser = argparse.ArgumentParser(description="Fetch Formula 1 data.")
parser.add_argument(
    "--start-year", type=int, default=2023, help="Start year for fetching data"
)
parser.add_argument(
    "--end-year", type=int, default=2024, help="End year for fetching data"
)
parser.add_argument(
    "--storage",
    choices=["local", "database"],
    default="local",
    help="Storage option for saving data (local or database)",
)

args = parser.parse_args()

START_YEAR = args.start_year
END_YEAR = args.end_year

# Check the value of the 'storage' argument
storage_option = args.storage

logger.info(f"Start Year: {START_YEAR}")
logger.info(f"End Year: {END_YEAR}")
logger.info(f"Storage Type: {storage_option}")

if storage_option == "database":
    # Read database configuration from config.json
    with open("config.json") as config_file:
        config = json.load(config_file)

    # Extract database configuration
    db_config = config["database"]

    logger.info(f"Database Configuration: {db_config}")

    # Establish a connection to the database
    try:
        import psycopg2

        conn = psycopg2.connect(
            dbname=db_config["database_name"],
            user=db_config["username"],
            password=db_config["password"],
            host=db_config["host"],
            port=db_config["port"],
        )

        logger.success(f"Database connection made")

    except Exception as e:
        logger.error(f"Error connecting to the database: {e}")

# Create directory for saving data if it doesn't exist
if not os.path.exists("Data"):
    os.makedirs("Data")

if not os.path.exists("Data/docker_volume"):
    os.makedirs("Data/docker_volume")

if not os.path.exists("Data/local_storage_csv"):
    os.makedirs("Data/local_storage_csv")

event_id = updating_event_id(storage_option)
driver_id = updating_driver_id(storage_option)
team_id = updating_team_id(storage_option)

for year in range(START_YEAR, END_YEAR + 1):
    logger.info(f"Fetching schedule for {year} year")
    try:
        schedule = fastf1.get_event_schedule(year)
        schedule = schedule.drop(
            [
                "Session1Date",
                "Session2Date",
                "Session3Date",
                "Session4Date",
                "Session5Date",
            ],
            axis=1,
        )
    except:
        logger.error(f"Cannot fetech schedule for {year}")
        continue

    # Add EventID (primary key)
    schedule["EventID"] = range(event_id, len(schedule) + event_id)
    schedule.insert(0, "EventID", schedule.pop("EventID"))
    event_id = len(schedule) + event_id

    if storage_option == "local":
        save_data_to_csv(schedule, "Data/local_storage_csv/schedule.csv")

    elif storage_option == "database":
        upload_data_to_table(schedule, "schedule", conn)

    for index, row in schedule.iterrows():
        logger.info(f"Fetching data for round {row['RoundNumber']} in {year} year")

        if (datetime.now() + timedelta(days=5)) < row["EventDate"]:
            logger.info(f"Data not available yet for {row['RoundNumber']}")
            continue

        # Fetching session data
        sessions = ["FP1", "FP2", "FP3", "S", "SS", "Q", "R"]
        for session_type in sessions:
            try:
                session = fastf1.get_session(year, row["RoundNumber"], session_type)
                session.load()
                results = session.results
                laps = session.laps
            except:
                logger.error(f"Session: {session_type}, does not exist")
                continue

            # Getting drivers
            drivers = results[["FullName", "Abbreviation"]].drop_duplicates("FullName")
            driver_id = update_drivers(drivers, driver_id)

            # Getting teams
            teams = results[["TeamName", "TeamColor"]].drop_duplicates(["TeamName"])
            team_id = update_teams(teams, team_id)

            # Droping unnecessary columns
            results = results.drop(
                [
                    "BroadcastName",
                    "Abbreviation",
                    "TeamColor",
                    "DriverId",
                    "FirstName",
                    "LastName",
                    "HeadshotUrl",
                    "TeamId",
                ],
                axis=1,
            )
            laps = laps.drop(
                [
                    "Sector1SessionTime",
                    "Sector2SessionTime",
                    "Sector3SessionTime"
                ],
                axis=1,
            )

            # Add EventID (reference key)  to session data
            results["EventID"] = row["EventID"]
            laps["EventID"] = row["EventID"]

            # Add SessionType as column
            results["SessionType"] = session_type
            laps["SessionType"] = session_type

            # Organizing laps and results
            results.insert(0, "EventID", results.pop("EventID"))
            laps.insert(0, "EventID", laps.pop("EventID"))
            results.insert(1, "SessionType", results.pop("SessionType"))
            laps.insert(1, "SessionType", laps.pop("SessionType"))

            # Append session data
            if storage_option == "local":
                save_data_to_csv(results, "Data/local_storage_csv/results.csv")
                save_data_to_csv(laps, "Data/local_storage_csv/laps.csv")

            elif storage_option == "database":
                upload_data_to_table(results, "results", conn)
                upload_data_to_table(laps, "laps", conn)
