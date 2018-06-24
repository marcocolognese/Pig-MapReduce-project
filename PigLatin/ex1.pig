-- What is the best day of week to fly to minimise delays?? (by flight number)
--SET default_parallel 2;
record = LOAD '/user/ross/input/2008.txt' using PigStorage(',') AS (Year:int,
Month:int,
DayofMonth:int,
DayOfWeek:int,
DepTime:chararray,
CRSDepTime:int,
ArrTime:int,
CRSArrTime:int,
UniqueCarrier:chararray,
FlightNum:int,
TailNum:chararray,
ActualElapsedTime:int,
CRSElapsedTime:int,
AirTime:int,
ArrDelay:int,
DepDelay:int,
Origin:chararray,
Dest:chararray,
Distance:int,
TaxiIn:int,
TaxiOut:int,
Cancelled:int,
CancellationCode:chararray,
Diverted:chararray,
CarrierDelay:chararray,
WeatherDelay:chararray,
NASDelay:chararray,
SecurityDelay:chararray,
LateAircraftDelay:chararray);

-- Average
averageGroup = GROUP record BY (FlightNum, DayOfWeek);
average = FOREACH averageGroup GENERATE group, AVG(record.ArrDelay);
flattenAverage = FOREACH average GENERATE FLATTEN(group.FlightNum) AS FlightNum, FLATTEN(group.DayOfWeek) AS DayOfWeek, $1 AS Average;

-- Minimum
minimumGroup = GROUP flattenAverage BY FlightNum;
minimum = FOREACH minimumGroup GENERATE group, MIN(flattenAverage.Average) AS Minimo;

bestDay = JOIN minimum BY (group, Minimo) , flattenAverage BY (FlightNum, Average);
bestDay1 = FOREACH bestDay GENERATE FlightNum, DayOfWeek, Average;

STORE bestDay1 INTO '/user/ross/outputPig/BestDay1';