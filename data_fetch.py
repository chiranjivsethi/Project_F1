import fastf1
import pandas as pd
import os
from datetime import datetime, timedelta
import argparse

parser = argparse.ArgumentParser(description='Fetch Formula 1 data.')
parser.add_argument('--start-year', type=int, default=2024, help='Start year for fetching data')
parser.add_argument('--end-year', type=int, default=2024, help='End year for fetching data')
args = parser.parse_args()

START_YEAR = args.start_year
END_YEAR = args.end_year

# Define function to save data
def save_data(data, filename):
    try:
        data.to_csv(filename, index=False, mode='a', header=not os.path.exists(filename))
        print(f"Data saved to {filename}")
    except Exception as e:
        print(f"Error saving data to {filename}: {e}")

# Create directory for saving data if it doesn't exist
if not os.path.exists('Data'):
    os.makedirs('Data')

# Initialize empty DataFrames for schedule and session data
schedule_df = pd.DataFrame()
race_results_df = pd.DataFrame()
race_laps_df = pd.DataFrame()
qualifying_results_df = pd.DataFrame()
qualifying_laps_df = pd.DataFrame()
practice_results_df = pd.DataFrame()
practice_laps_df = pd.DataFrame()
sprint_results_df = pd.DataFrame()
sprint_laps_df = pd.DataFrame()
sprint_shootout_results_df = pd.DataFrame()
sprint_shootout_laps_df = pd.DataFrame()

event_id = 1

for year in range(START_YEAR, END_YEAR+1):
    print(f"Fetching schedule for {year} year")
    schedule = fastf1.get_event_schedule(year)
    
    # Add EventID (primary key)
    schedule['EventID'] = range(event_id, len(schedule) + event_id)
    event_id = len(schedule) + event_id
    
    schedule_df = pd.concat([schedule_df, schedule], ignore_index=True)
    save_data(schedule_df, "Data/schedule.csv")
    
    for index, row in schedule.iterrows():
        print(f"Fetching data for round {row['RoundNumber']} in {year} year")
        
        # Fetching session data
        try:
            sessions = ['FP1', 'FP2', 'FP3', 'Q', 'R', 'S', 'SS']
            for session_type in sessions:
                session = fastf1.get_session(year, row['RoundNumber'], session_type)
                session.load()
                results = session.results
                laps = session.laps
                
                # Add EventID (reference key)  to session data
                results['EventID'] = row['EventID']
                laps['EventID'] = row['EventID']
                
                #Add SessionType as column
                results['SessionType'] = session_type
                laps['SessionType'] = session_type

                # Append session data
                if session_type.startswith("FP"):
                    practice_results_df = pd.concat([practice_results_df, results], ignore_index=True)
                    save_data(practice_results_df, "Data/practice_results.csv")
                
                    practice_laps_df = pd.concat([practice_laps_df, laps], ignore_index=True)
                    save_data(practice_laps_df, "Data/practice_laps.csv")
                    
                elif session_type == 'Q':
                    qualifying_results_df = pd.concat([qualifying_results_df, results], ignore_index=True)
                    save_data(qualifying_results_df, "Data/qualifying_results.csv")
                
                    qualifying_laps_df = pd.concat([qualifying_laps_df, laps], ignore_index=True)
                    save_data(qualifying_laps_df, "Data/qualifying_laps.csv")
                    
                elif session_type == 'R':
                    race_results_df = pd.concat([race_results_df, results], ignore_index=True)
                    save_data(race_results_df, "Data/race_results.csv")
                
                    race_laps_df = pd.concat([race_laps_df, laps], ignore_index=True)
                    save_data(race_laps_df, "Data/race_laps.csv")
                    
                elif session_type == 'S':
                    sprint_results_df = pd.concat([sprint_results_df, results], ignore_index=True)
                    save_data(sprint_results_df, "Data/sprint_results.csv")
                
                    sprint_laps_df = pd.concat([sprint_laps_df, laps], ignore_index=True)
                    save_data(sprint_laps_df, "Data/sprint_laps.csv")
                
                elif session_type == 'SS':
                    sprint_shootout_results_df = pd.concat([sprint_shootout_results_df, results], ignore_index=True)
                    save_data(sprint_shootout_results_df, "Data/sprint_shootout_results.csv")
                
                    sprint_shootout_laps_df = pd.concat([sprint_shootout_laps_df, laps], ignore_index=True)
                    save_data(sprint_shootout_laps_df, "Data/sprint_shootout_laps.csv")
                
        except Exception as e:
            print(f"Error fetching data for round {row['RoundNumber']} in {year}: {e}")

print("Data fetching and saving completed")