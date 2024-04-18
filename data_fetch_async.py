import asyncio
import aiofiles
#import aiopg
import asyncpg
import json
import os
import fastf1
from datetime import datetime, timedelta, timezone
from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor
import pandas as pd

async def setup_db_connection():
    with open("config.json") as config_file:
        config = json.load(config_file)
    db_config = config["database"]
    return await asyncpg.connect(
        dbname=db_config["database_name"],
        user=db_config["username"],
        password=db_config["password"],
        host=db_config["host"],
        port=db_config["port"]
    )

async def save_data_to_csv(data, filename):
    async with aiofiles.open(filename, mode='a') as file:
        await file.write(data.to_csv(index=False, header=not os.path.exists(filename)))

async def fetch_schedule(year, executor):
    try:
        loop = asyncio.get_running_loop()
        schedule = await loop.run_in_executor(executor, fastf1.get_event_schedule, year)
        return schedule
    except Exception as e:
        print(f"Cannot fetch schedule for {year}: {e}")
        return pd.DataFrame()  # Return empty DataFrame on failure

async def fetch_and_process_sessions(year, round_number, session_types, storage_option, conn, executor):
    tasks = [asyncio.create_task(fetch_and_process_session(year, round_number, session_type, storage_option, conn, executor))
             for session_type in session_types]
    await asyncio.gather(*tasks)

async def upload_data_to_table(dataframe, table_name, conn):
    if not conn:
        print("No connection provided.")
        return

    try:
        cursor = await conn.cursor()
        # Convert DataFrame to a list of tuples, replacing NaT, NaN, and empty strings with None
        data = [tuple(None if pd.isnull(value) or value == '' else value for value in row) for row in dataframe.to_numpy()]

        # Construct the SQL query for inserting data into the table
        columns = ", ".join(dataframe.columns)
        placeholders = ", ".join(["%s"] * len(dataframe.columns))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        # Execute the insert query using executemany
        await cursor.executemany(insert_query, data)
        await conn.commit()
        print(f"Data uploaded to '{table_name}' table successfully")

    except Exception as e:
        print(f"Error uploading data to '{table_name}' table: {e}")
        await conn.rollback()

    finally:
        if cursor:
            await cursor.close()

async def fetch_and_process_session(year, round_number, session_type, executor):
    loop = asyncio.get_running_loop()
    try:
        session = await loop.run_in_executor(executor, fastf1.get_session, year, round_number, session_type)
        session.load()
        results = session.results
        laps = session.laps
        # Add EventID, SessionType to both results and laps DataFrame before returning
        # Assume event_id and session_id are obtained or defined elsewhere
        results["SessionType"] = session_type
        laps["SessionType"] = session_type
        # Combine or handle results and laps as needed
        return results  # or laps depending on what needs to be merged
    except Exception as e:
        print(f"Session: {session_type}, does not exist for {year} R{round_number}: {e}")
        return None
    
async def fetch_and_merge_sessions(year, round_number, session_types, executor):
    tasks = [asyncio.create_task(fetch_and_process_session(year, round_number, session_type, executor))
             for session_type in session_types]
    session_dataframes = await asyncio.gather(*tasks)
    # Merge all dataframes into one, assuming they have the same structure
    combined_dataframe = pd.concat(session_dataframes, ignore_index=True)
    return combined_dataframe

async def fetch_and_process_sessions(year, round_number, storage_option, conn, executor):
    practice_sessions = ['FP1', 'FP2', 'FP3']
    race_weekend_sessions = ['S', 'Q', 'R']
    
    # Fetch and process practice sessions first
    practice_data = await fetch_and_merge_sessions(year, round_number, practice_sessions, executor)
    if practice_data is not None:
        if storage_option == "local":
            await save_data_to_csv(practice_data, f"Data/local_storage_csv/practice_sessions.csv")
        elif storage_option == "database":
            await upload_data_to_table(practice_data, "practice_sessions", conn)

    # Now process other sessions
    for session_type in race_weekend_sessions:
        session_data = await fetch_and_process_session(year, round_number, session_type, executor)
        if session_data is not None:
            if storage_option == "local":
                await save_data_to_csv(session_data, f"Data/local_storage_csv/{session_type}_sessions.csv")
            elif storage_option == "database":
                await upload_data_to_table(session_data, f"{session_type}_sessions", conn)

async def main():
    parser = ArgumentParser(description="Fetch Formula 1 data asynchronously.")
    parser.add_argument("--start-year", type=int, default=2024, help="Start year for fetching data")
    parser.add_argument("--end-year", type=int, default=2024, help="End year for fetching data")
    parser.add_argument("--storage", choices=["local", "database"], default="database", help="Storage option for saving data (local or database)")
    args = parser.parse_args()

    if not os.path.exists("Data"):
        os.makedirs("Data")
        os.makedirs("Data/docker_volume")
        os.makedirs("Data/local_storage_csv")

    executor = ThreadPoolExecutor(max_workers=10)  # Adjust the number of workers as needed
    conn = None
    if args.storage == 'database':
        conn = await setup_db_connection()

    tasks = [asyncio.create_task(fetch_and_process_sessions(year, round_number, args.storage, conn, executor))
             for year in range(args.start_year, args.end_year + 1)
             for round_number in range(1, 22)]  # Assuming a maximum of 21 rounds per year
    await asyncio.gather(*tasks)

    if conn:
        await conn.close()

    if __name__ == "__main__":
        asyncio.run(main())