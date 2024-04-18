-- Create the schedule table
CREATE TABLE IF NOT EXISTS schedule (
    EventID INT PRIMARY KEY,
    RoundNumber INT,
    Country VARCHAR(100),
    Location VARCHAR(100),
    OfficialEventName VARCHAR(100),
    EventDate TIMESTAMP,
    EventName VARCHAR(100),
    EventFormat VARCHAR(100),
    Session1 VARCHAR(100),
    Session1DateUtc TIMESTAMP,
    Session2 VARCHAR(100),
    Session2DateUtc TIMESTAMP,
    Session3 VARCHAR(100),
    Session3DateUtc TIMESTAMP,
    Session4 VARCHAR(100),
    Session4DateUtc TIMESTAMP,
    Session5 VARCHAR(100),
    Session5DateUtc TIMESTAMP,
    F1ApiSupport BOOLEAN
);

-- Create the laps table
CREATE TABLE IF NOT EXISTS laps (
    Time INTERVAL,
    Driver VARCHAR(100),
    DriverNumber INT,
    LapTime INTERVAL,
    LapNumber FLOAT,
    Stint FLOAT,
    PitOutTime INTERVAL,
    PitInTime INTERVAL,
    Sector1Time INTERVAL,
    Sector2Time INTERVAL,
    Sector3Time INTERVAL,
    Sector1SessionTime INTERVAL,
    Sector2SessionTime INTERVAL,
    Sector3SessionTime INTERVAL,
    SpeedI1 FLOAT,
    SpeedI2 FLOAT,
    SpeedFL FLOAT,
    SpeedST FLOAT,
    IsPersonalBest BOOLEAN,
    Compound VARCHAR(100),
    TyreLife FLOAT,
    FreshTyre BOOLEAN,
    Team VARCHAR(100),
    LapStartTime INTERVAL,
    LapStartDate TIMESTAMP,
    TrackStatus INT,
    Position INT,
    Deleted BOOLEAN,
    DeletedReason VARCHAR(100),
    FastF1Generated BOOLEAN,
    IsAccurate BOOLEAN,
    EventID INT,
    SessionType VARCHAR(100),
    PRIMARY KEY (EventID, SessionType, Team, Driver, DriverNumber, LapNumber),
    FOREIGN KEY (EventID) REFERENCES schedule(EventID)
);

-- Create the results table
CREATE TABLE IF NOT EXISTS results (
    DriverNumber INT,
    BroadcastName VARCHAR(100),
    Abbreviation VARCHAR(100),
    DriverId VARCHAR(100),
    TeamName VARCHAR(100),
    TeamColor VARCHAR(100),
    TeamId VARCHAR(100),
    FirstName VARCHAR(100),
    LastName VARCHAR(100),
    FullName VARCHAR(100),
    HeadshotUrl VARCHAR(255),
    CountryCode VARCHAR(3),
    Position INT,
    ClassifiedPosition VARCHAR(2),
    GridPosition INT,
    Q1 INTERVAL,
    Q2 INTERVAL,
    Q3 INTERVAL,
    Time INTERVAL,
    Status VARCHAR(100),
    Points FLOAT,
    EventID INT,
    SessionType VARCHAR(100),
    PRIMARY KEY (EventID, SessionType, DriverId),
    FOREIGN KEY (EventID) REFERENCES schedule(EventID)
);
