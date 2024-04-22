from loguru import logger
import pandas as pd
import os

# Save data to CSV
def save_data_to_csv(data, filename):
    try:
        data.to_csv(
            filename, index=False, mode="a", header=not os.path.exists(filename)
        )
        logger.success(f"Data saved to {filename}")
    except Exception as e:
        logger.error(f"Error saving data to {filename}: {e}")

# Save data to database table
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
            
def updating_event_id(storage_option, conn=None):
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