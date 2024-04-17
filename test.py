import fastf1

session = fastf1.get_session(2023, 12, "S")
session.load()
results = session.results
laps = session.laps

print(results)
print("-----------------------------------------------")
print(laps)