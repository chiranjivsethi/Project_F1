-- Create the schedule table
CREATE TABLE IF NOT EXISTS schedule (
    EventID SERIAL PRIMARY KEY,
    RoundNumber INT,
    Country VARCHAR(100),
    Location VARCHAR(100),
    OfficialEventName VARCHAR(200),
    EventDate DATE,
    EventName VARCHAR(200),
    EventFormat VARCHAR(100),
    Session1 VARCHAR(100),
    Session1Date TIMESTAMP WITH TIME ZONE,
    Session1DateUtc TIMESTAMP WITH TIME ZONE, -- New column for Session1Date UTC
    Session2 VARCHAR(100),
    Session2Date TIMESTAMP WITH TIME ZONE,
    Session2DateUtc TIMESTAMP WITH TIME ZONE, -- New column for Session2Date UTC
    Session3 VARCHAR(100),
    Session3Date TIMESTAMP WITH TIME ZONE,
    Session3DateUtc TIMESTAMP WITH TIME ZONE, -- New column for Session3Date UTC
    Session4 VARCHAR(100),
    Session4Date TIMESTAMP WITH TIME ZONE,
    Session4DateUtc TIMESTAMP WITH TIME ZONE, -- New column for Session4Date UTC
    Session5 VARCHAR(100),
    Session5Date TIMESTAMP WITH TIME ZONE,
    Session5DateUtc TIMESTAMP WITH TIME ZONE, -- New column for Session5Date UTC
    F1ApiSupport BOOLEAN
);

-- Create the laps table
CREATE TABLE IF NOT EXISTS laps (
    EventID INT,
    SessionType VARCHAR(10),
    Time INTERVAL,
    Driver VARCHAR(50),
    DriverNumber INT,
    LapTime INTERVAL,
    LapNumber INT,
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
    Compound VARCHAR(20),
    TyreLife FLOAT,
    FreshTyre BOOLEAN,
    Team VARCHAR(50),
    LapStartTime INTERVAL,
    LapStartDate TIMESTAMP WITHOUT TIME ZONE,
    TrackStatus INT,
    Position INT,
    Deleted BOOLEAN,
    DeletedReason VARCHAR(50),
    FastF1Generated BOOLEAN,
    IsAccurate BOOLEAN
);

-- Create the results table
CREATE TABLE IF NOT EXISTS results (
    EventID INT,
    SessionType VARCHAR(10),
    DriverNumber INT,
    BroadcastName VARCHAR(50),
    Abbreviation VARCHAR(10),
    DriverId VARCHAR(50),
    TeamName VARCHAR(100),
    TeamColor VARCHAR(10),
    TeamId VARCHAR(50),
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    FullName VARCHAR(100),
    HeadshotUrl VARCHAR(255),
    CountryCode VARCHAR(10),
    Position INT,
    ClassifiedPosition VARCHAR(10),
    GridPosition INT,
    Q1 INTERVAL,
    Q2 INTERVAL,
    Q3 INTERVAL,
    Time INTERVAL,
    Status VARCHAR(50),
    Points FLOAT
);