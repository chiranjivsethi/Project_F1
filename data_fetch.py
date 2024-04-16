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

if not os.path.exists('Data'):
    os.makedirs('Data')
    
years = []
schedules = []
race_results = []
race_laps = []
qualifying_results = []
qualifying_laps = []
practic1_results = []
practic1_laps = []
practic2_results = []
practic2_laps = []
practic3_results = []
practic3_laps = []
sprint_results = []
sprint_laps = []
sprint_shootout_results = []
sprint_shootout_laps = []

for year in range(START_YEAR, END_YEAR+1):
    print(f"Fetching schedule for {year} year")
    years.append(year)
    
    # Fetching event schedules
    schedule = fastf1.get_event_schedule(year)
    schedules.append(schedule)
    print(f"Schedule downlaoded")
    print("--------------------------------------------------")
    
schedules = pd.concat(schedules)
# Adding EventID
schedules.reset_index(drop=True, inplace=True)
schedules['EventID'] = schedules.index + 1
#Saving schedules
schedules.to_csv("Data/schedules.csv", index=False)

#Saving years
years = pd.DataFrame(years, columns=['year'])
years.to_csv("Data/years_held.csv", index=False)

for index, row in schedules.iterrows():
    print(f"Fetching results and laps data for {row['RoundNumber']} round in {row['EventDate'].year} year")
    
    #If Round is Pre-Season Testing then skip
    if row['RoundNumber'] == 0:
        print("Cannot get testing data...")
        print("--------------------------------------------------")
        continue
    
    #If Round was taken place within 5 days then skip
    elif row['EventDate'] > datetime.today() + timedelta(hours=5*24):
        print("Data not available")
        print("--------------------------------------------------")
        continue
    
    #Get free practice 1 result and laps data
    print("Fetching data for FP1")
    session = fastf1.get_session(row['EventDate'].year, row['RoundNumber'], 'FP1')
    session.load()
    practic1_result = session.results
    practic1_result['EventID'] = row['EventID']
    practic1_results.append(practic1_result)
    practic1_lap = session.laps
    practic1_lap['EventID'] = row['EventID']
    practic1_laps.append(practic1_lap)
    print("FP1 data downloaded")
    
    #Get free practice 2 result and laps data
    try:
        print("Fetching data for FP2")
        session = fastf1.get_session(row['EventDate'].year, row['RoundNumber'], 'FP2')
        session.load()
        practic2_result = session.results
        practic2_result['EventID'] = row['EventID']
        practic2_results.append(practic2_result)
        practic2_lap = session.laps
        practic2_lap['EventID'] = row['EventID']
        practic2_laps.append(practic2_lap)
        print("FP2 data downloaded")
    except ValueError:
        print("FP2 Session did not take place for this event")
        
    #Get free practice 3 result and laps data
    try:
        print("Fetching data for FP3")
        session = fastf1.get_session(row['EventDate'].year, row['RoundNumber'], 'FP3')
        session.load()
        practic3_result = session.results
        practic3_result['EventID'] = row['EventID']
        practic3_results.append(practic3_result)
        practic3_lap = session.laps
        practic3_lap['EventID'] = row['EventID']
        practic3_laps.append(practic3_lap)
        print("FP3 data downloaded")
    except ValueError:
        print("FP3 Session did not take place for this event")
    
    #Get Sprint Shootout result and laps data
    try:
        print("Fetching data for Sprint Shootout")
        session = fastf1.get_session(row['EventDate'].year, row['RoundNumber'], 'SS')
        session.load()
        sprint_shootout_result = session.results
        sprint_shootout_result['EventID'] = row['EventID']
        sprint_shootout_results.append(sprint_shootout_result)
        sprint_shootout_lap = session.laps
        sprint_shootout_lap['EventID'] = row['EventID']
        sprint_shootout_laps.append(sprint_shootout_lap)
        print("Sprint Shootout data downloaded")
    except ValueError:
        print("Sprint Shootout Session did not take place for this event")
        
    #Get Sprint result and laps data
    try:
        print("Fetching data for Sprint")
        session = fastf1.get_session(row['EventDate'].year, row['RoundNumber'], 'S')
        session.load()
        sprint_result = session.results
        sprint_result['EventID'] = row['EventID']
        sprint_results.append(sprint_result)
        sprint_lap = session.laps
        sprint_lap['EventID'] = row['EventID']
        sprint_laps.append(sprint_lap)
        print("Sprint data downloaded")
    except ValueError:
        print("Sprint Session did not take place for this event")
    
    #Get qualifying result and laps data
    print("Fetching data for Qualifying")
    session = fastf1.get_session(row['EventDate'].year, row['RoundNumber'], 'Q')
    session.load()
    qualifying_result = session.results
    qualifying_result['EventID'] = row['EventID']
    qualifying_results.append(qualifying_result)
    qualifying_lap = session.laps
    qualifying_lap['EventID'] = row['EventID']
    qualifying_laps.append(qualifying_lap)
    print("Qualifying data downloaded")
        
    #Get race result and laps data
    print("Fetching data for Race")
    session = fastf1.get_session(row['EventDate'].year, row['RoundNumber'], 'R')
    session.load()
    race_result = session.results
    race_result['EventID'] = row['EventID']
    race_results.append(race_result)
    race_lap = session.laps
    race_lap['EventID'] = row['EventID']
    race_laps.append(race_lap)
    print("Race data downloaded")
    
    print(f"Round result downlaoded")
    print("--------------------------------------------------")

try:
    race_laps = pd.concat(race_laps)
    race_laps.to_csv("Data/race_laps.csv", index=False)
except:
    print("No race laps found")

try:
    race_results = pd.concat(race_results)
    race_results.to_csv("Data/race_results.csv", index=False)
except:
    print("No race results found")

try:
    qualifying_laps = pd.concat(qualifying_laps)
    qualifying_laps.to_csv("Data/qualifying_laps.csv", index=False)
except:
    print("No qualifying laps found")

try:
    qualifying_results = pd.concat(qualifying_results)
    qualifying_results.to_csv("Data/qualifying_results.csv", index=False)
except:
    print("No qualifying results found")
    
try:
    sprint_shootout_laps = pd.concat(sprint_shootout_laps)
    sprint_shootout_laps.to_csv("Data/sprint_shootout_laps.csv", index=False)
except:
    print("No sprint shootout laps found")

try:
    sprint_shootout_results = pd.concat(sprint_shootout_results)
    sprint_shootout_results.to_csv("Data/sprint_shootout_results.csv", index=False)
except:
    print("No sprint shootout results found")

try:
    sprint_laps = pd.concat(sprint_laps)
    sprint_laps.to_csv("Data/sprint_laps.csv", index=False)
except:
    print("No sprint laps found")

try:
    sprint_results = pd.concat(sprint_results)
    sprint_results.to_csv("Data/sprint_results.csv", index=False)
except:
    print("No sprint results found")

try:
    practic1_laps = pd.concat(practic1_laps)
    practic1_laps.to_csv("Data/practic1_laps.csv", index=False)
except:
    print("No FP1 laps found")

try:
    practic1_results = pd.concat(practic1_results)
    practic1_results.to_csv("Data/practic1_results.csv", index=False)
except:
    print("No FP1 results found")

try:
    practic2_laps = pd.concat(practic2_laps)
    practic2_laps.to_csv("Data/practic2_laps.csv", index=False)
except:
    print("No FP2 laps found")

try:
    practic2_results = pd.concat(practic2_results)
    practic2_results.to_csv("Data/practic2_results.csv", index=False)
except:
    print("No FP2 results found")

try:
    practic3_laps = pd.concat(practic3_laps)
    practic3_laps.to_csv("Data/practic3_laps.csv", index=False)
except:
    print("No FP3 laps found")
    
try:
    practic3_results = pd.concat(practic3_results)
    practic3_results.to_csv("Data/practic3_results.csv", index=False)
except:
    print("No FP3 results found")