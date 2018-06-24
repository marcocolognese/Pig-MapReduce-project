-- How well does weather predict plane delays?
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

-- Arrival delay
recordOk = FILTER record BY (WeatherDelay != 'NA' AND WeatherDelay != '' AND WeatherDelay IS NOT NULL);
newRecord = FOREACH recordOk GENERATE Year, Month, DayofMonth, DayOfWeek, DepTime, CRSDepTime, ArrTime, CRSArrTime, UniqueCarrier, FlightNum, TailNum, ActualElapsedTime, CRSElapsedTime, AirTime, ArrDelay, DepDelay, Origin, Dest, Distance, TaxiIn, TaxiOut, Cancelled, CancellationCode, Diverted, CarrierDelay, (int)WeatherDelay, NASDelay, SecurityDelay, LateAircraftDelay;
averageGroup = GROUP newRecord ALL;
arrivalAverage = FOREACH averageGroup GENERATE 'Arrival delay:' AS String, AVG(newRecord.ArrDelay) AS Average;

-- Weather delay
weatherAverage = FOREACH averageGroup GENERATE 'Weather delay:' AS String, AVG(newRecord.WeatherDelay) AS Average;

result = UNION arrivalAverage, weatherAverage;
STORE result INTO '/user/ross/outputPig/WeatherDelay';