import fastf1
import numpy as np
import pandas as pd
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
    default="database",
    help="Storage option for saving data (local or database)",
)

args = parser.parse_args()

START_YEAR = args.start_year
END_YEAR = args.end_year

# Check the value of the 'storage' argument
storage_option = args.storage

if storage_option == 'database':
    # Read database configuration from config.json
    with open("config.json") as config_file:
        config = json.load(config_file)

    # Extract database configuration
    db_config = config["database"]

    # Establish a connection to the database
    try:
        import psycopg2

        conn = psycopg2.connect(
            dbname=db_config["database_name"],
            user=db_config["username"],
            password=db_config["password"],
            host=db_config["host"],
            port=db_config["port"]
        )
        print("Connected to the database")
        
    except Exception as e:
        print(f"Error connecting to the database: {e}")

# Define function to save data to CSV
def save_data_to_csv(data, filename):
    try:
        data.to_csv(
            filename, index=False, mode="a", header=not os.path.exists(filename)
        )
        print(f"Data saved to {filename}")
    except Exception as e:
        print(f"Error saving data to {filename}: {e}")
        
def upload_data_to_table(dataframe, table_name, conn=None):
    
    if conn:
        print("Conneection provided")
    else:
        print("No Conneection provided")
    
    cursor = None
    
    try:
        # Convert DataFrame to a list of tuples, replacing NaT with None
        data = [tuple(None if pd.isnull(value) else value for value in row) for row in dataframe.to_numpy()]
        
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
        print(f"Data uploaded to '{table_name}' table successfully")
        
    except Exception as e:
        print(f"Error uploading data to '{table_name}' table: {e}")
        conn.rollback()
        
    finally:
        # Close cursor and connection
        if cursor:
            cursor.close()

# Create directory for saving data if it doesn't exist
if not os.path.exists("Data"):
    os.makedirs("Data")

event_id = 1
    
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
    
    if storage_option == "local":
        save_data_to_csv(schedule, "Data/schedule.csv")
    
    elif storage_option == "database":
        upload_data_to_table(schedule, "schedule", conn)

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
            if storage_option == "local":
                save_data_to_csv(results, "Data/results.csv")
                save_data_to_csv(laps, "Data/laps.csv")
            
            elif storage_option == "database":
                upload_data_to_table(results, "results", conn)
                upload_data_to_table(laps, "laps", conn)
                
if conn:
    conn.close()