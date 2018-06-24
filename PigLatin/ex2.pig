-- What is the best day of week to fly to minimise delays?? (in general)
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
averageGroup = GROUP record BY (DayOfWeek);
average = FOREACH averageGroup GENERATE group, AVG(record.ArrDelay);
flattenAverage = FOREACH average GENERATE FLATTEN(group) AS DayOfWeek, $1 AS Average;

-- Minimum
minimumGroup = GROUP flattenAverage ALL;
minimum = FOREACH minimumGroup GENERATE group, MIN(flattenAverage.Average) AS Minimo;

bestDay = JOIN minimum BY Minimo, flattenAverage BY Average;
bestDay1 = FOREACH bestDay GENERATE DayOfWeek, Average;

STORE bestDay1 INTO '/user/ross/outputPig/BestDay2';