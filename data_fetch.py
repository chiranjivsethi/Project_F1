import fastf1
import pandas as pd
import os
from datetime import datetime, timedelta
import argparse
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import json

parser = argparse.ArgumentParser(description="Fetch Formula 1 data.")
parser.add_argument(
    "--start-year", type=int, default=2024, help="Start year for fetching data"
)
parser.add_argument(
    "--end-year", type=int, default=2024, help="End year for fetching data"
)
parser.add_argument(
    "--storage",
    choices=["local", "postgres"],
    default="local",
    help="Storage option for saving data (local or postgres)",
)

args = parser.parse_args()

START_YEAR = args.start_year
END_YEAR = args.end_year

# Check the value of the 'storage' argument
storage_option = args.storage

# Read database configuration from config.json
with open("config.json") as config_file:
    config = json.load(config_file)

# Extract database configuration
db_config = config["database"]

# Initialize SQLAlchemy engine if PostgreSQL storage is chosen
if storage_option == "postgres":
    engine = create_engine(
        f"{db_config['drivername']}://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database_name']}"
    )
    Base = declarative_base()

    class SessionData(Base):
        __tablename__ = "session_data"

        id = Column(Integer, primary_key=True)
        event_id = Column(Integer)
        session_type = Column(String)
        # Add more columns as needed based on your data

        def __repr__(self):
            return f"<SessionData(event_id={self.event_id}, session_type={self.session_type}, ...)>"  # Add other columns here

    # Create tables
    Base.metadata.create_all(engine)


# Define function to save data to CSV
def save_data_to_csv(data, filename):
    try:
        data.to_csv(
            filename, index=False, mode="a", header=not os.path.exists(filename)
        )
        print(f"Data saved to {filename}")
    except Exception as e:
        print(f"Error saving data to {filename}: {e}")


# Define function to save data to PostgreSQL using SQLAlchemy
def save_data_to_sqlalchemy(data, model):
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        for index, row in data.iterrows():
            session.add(model(**row.to_dict()))

        session.commit()
        print(f"Data saved to {model.__tablename__}")

    except Exception as e:
        session.rollback()
        print(f"Error saving data to {model.__tablename__}: {e}")

    finally:
        session.close()


# Create directory for saving data if it doesn't exist
if not os.path.exists("Data"):
    os.makedirs("Data")

event_id = 1

# Depending on storage option, choose the saving function
if storage_option == "local":
    save_data_function = save_data_to_csv
elif storage_option == "postgres":
    save_data_function = save_data_to_sqlalchemy

for year in range(START_YEAR, END_YEAR + 1):
    print(f"Fetching schedule for {year} year")
    schedule = fastf1.get_event_schedule(year)

    # Add EventID (primary key)
    schedule["EventID"] = range(event_id, len(schedule) + event_id)
    event_id = len(schedule) + event_id

    save_data_function(schedule, "Data/schedule.csv")
    schedule_df = pd.DataFrame()

    for index, row in schedule.iterrows():
        print(f"Fetching data for round {row['RoundNumber']} in {year} year")

        # Fetching session data
        try:
            sessions = ["FP1", "FP2", "FP3", "Q", "R", "S", "SS"]
            for session_type in sessions:
                session = fastf1.get_session(year, row["RoundNumber"], session_type)
                session.load()
                results = session.results
                laps = session.laps

                # Add EventID (reference key)  to session data
                results["EventID"] = row["EventID"]
                laps["EventID"] = row["EventID"]

                # Add SessionType as column
                results["SessionType"] = session_type
                laps["SessionType"] = session_type

                # Append session data
                if session_type.startswith("FP"):
                    save_data_function(results, "Data/practice_results.csv")
                    save_data_function(laps, "Data/practice_laps.csv")

                elif session_type == "Q":
                    save_data_function(results, "Data/qualifying_results.csv")
                    save_data_function(laps, "Data/qualifying_laps.csv")

                elif session_type == "R":
                    save_data_function(results, "Data/race_results.csv")
                    save_data_function(laps, "Data/race_laps.csv")

                elif session_type == "S":
                    save_data_function(results, "Data/sprint_results.csv")
                    save_data_function(laps, "Data/sprint_laps.csv")

                elif session_type == "SS":
                    save_data_function(results, "Data/sprint_shootout_results.csv")
                    save_data_function(laps, "Data/sprint_shootout_laps.csv")

        except Exception as e:
            print(f"Error fetching data for round {row['RoundNumber']} in {year}: {e}")

print("Data fetching and saving completed")
