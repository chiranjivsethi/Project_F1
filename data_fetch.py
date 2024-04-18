import fastf1
import pandas as pd
import os
from datetime import datetime, timedelta, timezone
import argparse
import json
from loguru import logger

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
    default="database",
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


# Define function to save data to CSV
def save_data_to_csv(data, filename):
    try:
        data.to_csv(
            filename, index=False, mode="a", header=not os.path.exists(filename)
        )
        logger.success(f"Data saved to {filename}")
    except Exception as e:
        logger.error(f"Error saving data to {filename}: {e}")


def upload_data_to_table(dataframe, table_name, conn=None):

    if conn:
        logger.info("Connection exist")
    else:
        logger.critical("Conneection does not exist")

    cursor = None

    try:
        # Convert DataFrame to a list of tuples, replacing NaT, NaN and empty strings with None
        data = [
            tuple(None if pd.isnull(value) or value == "" else value for value in row)
            for row in dataframe.to_numpy()
        ]

        # Create a cursor
        cursor = conn.cursor()

        # Construct the SQL query for inserting data into the table
        columns = ", ".join(dataframe.columns)
        placeholders = ", ".join(["%s"] * len(dataframe.columns))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        # Execute the insert query
        cursor.executemany(insert_query, data)

        # Commit the transaction
        conn.commit()
        logger.success(f"Data uploaded to '{table_name}' table successfully")

    except Exception as e:
        logger.error(f"Error uploading data to '{table_name}' table: {e}")
        conn.rollback()

    finally:
        # Close cursor and connection
        if cursor:
            cursor.close()


def updating_event_id():
    if storage_option == "local":
        try:
            df = pd.read_csv("./Data/local_storage_csv/schedule.csv")
            return int(df["EventID"].max()) + 1
        except FileNotFoundError:
            return 1
    elif storage_option == "database":
        event_id = pd.read_sql_query(
            "select max(EventID) from schedule;", con=conn
        ).iloc[0, 0]

        if event_id == None:
            return 1

        return event_id + 1


def updating_driver_id():
    if storage_option == "local":
        df = pd.read_csv("./Data/local_storage_csv/schedule.csv")
        return int(df["EventID"].max())
    elif storage_option == "database":
        return int(
            pd.read_sql_query("select max(EventID) from schedule;", con=conn).iloc[0, 0]
        )


def updating_team_id():
    if storage_option == "local":
        df = pd.read_csv("./Data/local_storage_csv/schedule.csv")
        return int(df["EventID"].max())
    elif storage_option == "database":
        return int(
            pd.read_sql_query("select max(EventID) from schedule;", con=conn).iloc[0, 0]
        )


# Create directory for saving data if it doesn't exist
if not os.path.exists("Data"):
    os.makedirs("Data")

if not os.path.exists("Data/docker_volume"):
    os.makedirs("Data/docker_volume")

if not os.path.exists("Data/local_storage_csv"):
    os.makedirs("Data/local_storage_csv")

event_id = updating_event_id()
driver_id = updating_driver_id()
team_id = updating_team_id()

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
                "F1ApiSupport",
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
            drivers = results[["fullname", "abbreviation"]].drop_duplicates("fullname")
            drivers["DriverID"] = range(driver_id, len(drivers) + driver_id)

            # Getting teams
            teams = results[["teamname", "teamcolor"]].drop_duplicates(["teamname"])

            # Droping unnecessary columns
            results = results.drop(
                [
                    "broadcastname",
                    "driverid",
                    "firstname",
                    "lastname",
                    "headshoturl",
                    "teamid",
                ],
                axis=1,
            )
            laps = laps.drop(
                [
                    "sector1sessiontime",
                    "sector2sessiontime",
                    "sector3sessiontime",
                    "lapstarttime",
                    "lapstartdate",
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
            laps.insert(0, "EventID", schedule.pop("EventID"))
            laps.insert(0, "EventID", schedule.pop("EventID"))
            laps.insert(1, "SessionType", schedule.pop("SessionType"))
            laps.insert(1, "SessionType", schedule.pop("SessionType"))

            # Append session data
            if storage_option == "local":
                save_data_to_csv(results, "Data/local_storage_csv/results.csv")
                save_data_to_csv(laps, "Data/local_storage_csv/laps.csv")

            elif storage_option == "database":
                upload_data_to_table(results, "results", conn)
                upload_data_to_table(laps, "laps", conn)
