-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw
LOCATION "abfss:/<container>@<storage_account>.dfs.core.windows.net/"

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "abfss://<container>@<storage_account>.dfs.core.windows.net/"

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "abfss://<container>@<storage_account>.dfs.core.windows.net/"

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(circuitId INT,
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING
)
USING csv
OPTIONS (path "abfss://<container>@<storage_account>.dfs.core.windows.net/circuits.csv", header 'true')

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(raceId INT,
year INT,
round INT,
circuitId INT,
name STRING,
date DATE,
time STRING,
url STRING)
USING csv
OPTIONS (path "abfss://<container>@<storage_account>.dfs.core.windows.net/races.csv", header true)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING)
USING csv
OPTIONS(path "abfss://<container>@<storage_account>.dfs.core.windows.net/constructors.csv", header "true")

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
driverId INT,
driverRef STRING,
number INT,
code STRING,
forename STRING,
surname STRING,
dob DATE,
nationality STRING,
url STRING)
USING csv
OPTIONS (
  path "abfss://<container>@<storage_account>.dfs.core.windows.net/drivers.csv",
  header "true"
)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,grid INT,
position INT,
positionText STRING,
positionOrder INT,
points INT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed FLOAT,
statusId STRING)
USING csv
OPTIONS(path "abfss://<container>@<storage_account>.dfs.core.windows.net/results.csv",
  header "true")
