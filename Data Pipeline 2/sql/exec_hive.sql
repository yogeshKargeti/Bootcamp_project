USE project_database;

CREATE TABLE IF NOT EXISTS cycle_data ( datetimestamp timestamp,
season String,
weather String,
working_day int,
temperature int,
humidity int,
windspeed int,
user_id int,
cycle_count int)
COMMENT ‘Employee details’
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ‘\t’
LINES TERMINATED BY ‘\n’
STORED AS TEXTFILE
LOCATION 'hdfs://user/data/cycle_data.csv';


CREATE TABLE IF NOT EXISTS mid_data
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ‘\t’
LINES TERMINATED BY ‘\n’
STORED AS TEXTFILE
AS SELECT date_format(datetimestamp,'mm') AS month,
date_format(datetimestamp,'yyyy') AS year,
season, weather, working_day int,
cycle_count int FROM cycle_data;


CREATE TABLE IF NOT EXISTS analysis_data
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ‘\t’
LINES TERMINATED BY ‘\n’
STORED AS TEXTFILE
SELECT month,year,season,weather,
SUM(CASE WHEN working_day = 1 THEN cycle_count ELSE 0) AS total_count_working_day,
SUM(CASE WHEN working_day = 0 THEN cycle_count ELSE 0) AS total_count_holiday)
FROM mid_data
GROUP BY month,year,season,weather;


CREATE TABLE IF NOT EXISTS year_partitioned_table ( month int,
season STRING,
weather STRING,
total_count_working_day int,
total_count_holiday int
)
PARTITIONED BY(year int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ‘\t’
LINES TERMINATED BY ‘\n’
STORED AS TEXTFILE;


INSERT INTO year_partitioned_table PARTITION(year)
SELECT month,season,weather,total_count_working_day,total_count_holiday,year from analysis_data;



