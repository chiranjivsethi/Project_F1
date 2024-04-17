import fastf1
import os
from datetime import datetime, timedelta
import argparse
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
    choices=["local", "database"],
    default="local",
    help="Storage option for saving data (local or database)",
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

# Define function to save data to CSV
def save_data_to_csv(data, filename):
    try:
        data.to_csv(
            filename, index=False, mode="a", header=not os.path.exists(filename)
        )
        print(f"Data saved to {filename}")
    except Exception as e:
        print(f"Error saving data to {filename}: {e}")

# Create directory for saving data if it doesn't exist
if not os.path.exists("Data"):
    os.makedirs("Data")

event_id = 1

# Depending on storage option, choose the saving function
if storage_option == "local":
    save_data_function = save_data_to_csv
elif storage_option == "database":
    pass

for year in range(START_YEAR, END_YEAR + 1):
    print(f"Fetching schedule for {year} year")
    try:
        schedule = fastf1.get_event_schedule(year)
    except:
        print(f"Cannot fetech schedule for {year}")
        continue

    # Add EventID (primary key)
    schedule["EventID"] = range(event_id, len(schedule) + event_id)
    event_id = len(schedule) + event_id
    
    save_data_function(schedule, "Data/schedule.csv")

    for index, row in schedule.iterrows():
        print(f"Fetching data for round {row['RoundNumber']} in {year} year")

        if (datetime.now() - timedelta(days=5)) < row["EventDate"]:
            print(f"Data not avalable for {row['RoundNumber']}")
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
                print(f"Session: {session_type}, does not exist")
                continue

            # Add EventID (reference key)  to session data
            results["EventID"] = row["EventID"]
            laps["EventID"] = row["EventID"]

            # Add SessionType as column
            results["SessionType"] = session_type
            laps["SessionType"] = session_type

            # Append session data
            save_data_function(results, "Data/results.csv")
            save_data_function(laps, "Data/laps.csv")

print("Data fetching and saving completed")