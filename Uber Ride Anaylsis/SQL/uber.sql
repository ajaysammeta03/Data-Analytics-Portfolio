# creating data base 
create database ride_sharing_db;

# using the data base
use ride_sharing_db;

# Preview all ride records including trip times, zones, fare, ratings, and status.
select * from rides_data;
# View all event records that may impact ride patterns, including date and zone of influence.
select * from event_data;
# Retrieve historical weather data to analyze how rain or temperature affects ride behavior.
select * from weather_data;

# Identify how many rides are missing a RideStatus value (i.e., null entries).
# This helps in assessing data quality before performing status-based analyses
select count(ridestatus) from rides_data
where ridestatus is null;


# Begin a transaction to safely apply updates to cancellation reasons.
start transaction;

# Set a specific reason for rides cancelled by customers for better clarity in analysis
update rides_data
set CancellationReason ='Customer Initiated Cancellation'
where RideStatus='Cancelled by Customer';  

# Disable SQL safe updates temporarily to allow updates without primary key conditions
set sql_safe_updates=0;

# Remove cancellation reason from rides that were successfully completed,as cancellation doesn't apply to them
update rides_data
set CancellationReason =null
where RideStatus='Completed';

# Commit the transaction to apply the above changes permanently.
commit;


# Calculate the trip duration in minutes for each ride using pickup and dropoff timestamps.
# Useful for analyzing average trip length, delays, and comparing estimated vs actual duration.
SELECT
    TIMESTAMPDIFF(MINUTE, PickupDateTime, DropoffDateTime) AS TripDurationMinutes
FROM  rides_data ;


# Identify data quality issues where the dropoff time is earlier than the pickup time.
# These records may indicate incorrect timestamp entries or data entry errors.
SELECT COUNT(*)
FROM rides_data
WHERE DropoffDateTime < PickupDateTime;


# Detect duplicate RideID entries, which may indicate data duplication issues.
# Important for ensuring data integrity before performing aggregations or joins.
SELECT RideID, COUNT(*)
FROM rides_data
GROUP BY RideID
HAVING COUNT(*) > 1;


# Standardize and optimize data types in the rides_data table for accuracy, efficiency, and consistency.
alter table rides_data


#Use VARCHAR for unique identifiers 
#Ensure datetime fields are properly typed for time-based analysis.
#Set appropriate lengths for location-related fields.
#Use DECIMAL with precision for geographic coordinates.
#Integer for distance and time-based numeric fields.
#Use DECIMAL for monetary and multiplier fields.
#Expand RideStatus and CancellationReason for descriptive values.
#Integer fields for 1–5 star rating values.
modify RideID varchar(20),
modify PickupDateTime datetime,
modify DropoffDateTime datetime,
modify PickupZone varchar(30),
modify PickupLatitude decimal(9,6),
modify PickupLongitude decimal(9,6),
modify DropoffZone varchar(30),
modify DropoffLatitude decimal(9,6),
modify DropoffLongitude decimal(9,6),
modify DistanceKM int,
modify FareAmount decimal(10,2),
modify SurgeMultiplier decimal(10,2),
modify RideStatus varchar(40),
modify WaitingTimeMinutes int,
modify EstimatedTripDurationMinutes int,
modify ActualTripDurationMinutes int,
modify DriverID varchar(30),
modify CustomerID varchar(30),
modify CancellationReason varchar(50),
modify DriverRating int,
modify CustomerRating int;


# Display the structure of the rides_data table including column names, data types, and constraints.
# Useful for understanding schema design and verifying if types align with analytical requirements.
describe rides_data;


# Preview all records in the events_data table to understand the structure and contents.
# Useful for identifying which events may impact ride behavior.
select * from events_data;

# Update the EventName column to ensure it supports longer or more descriptive event titles.
# Helps standardize the schema and prevents truncation of event names.
alter  table events_data
modify EventName varchar(50),
modify ImpactZone varchar(30),
modify ImpactRadiusKM int;

# Add a new column to store event dates in DATE format.
# This enables accurate joins with rides_data on date fields for event impact analysis
alter table events_data
add event_date date;


# Populate the new event_date column by converting the existing 'date' string (in DD-MM-YYYY format) to proper DATE type.
# This step ensures compatibility for date-based joins and time-series analysis.
UPDATE events_data
SET event_date = STR_TO_DATE(date, '%d-%m-%Y');

# Remove the original 'date' column now that its values have been safely converted and stored in 'event_date'.
# This avoids redundancy and keeps the schema clean
alter table events_data
drop column date;


# Adjust data types in weather_data for improved accuracy and consistency.
alter table weather_data
 # Increase precision for temperature values to handle more accurate measurements.
modify TemperatureC decimal(9,6),
# Ensure the 'IsRaining' field supports consistent yes/no or true/false values.
modify IsRaining varchar(10);


# Analyze ride performance metrics by ride status.
# This provides insights into how trip duration and wait time vary across completed, cancelled, or other ride outcomes.
SELECT
  RideStatus,
  AVG(ActualTripDurationMinutes) AS avg_trip_duration,
  AVG(WaitingTimeMinutes) AS avg_waiting_time
FROM rides_data
GROUP BY RideStatus;


# Analyze ride distribution across different hours of the day.
# This helps identify peak ride hours and supports operational planning, such as driver allocation.
SELECT 
  HOUR(PickupDateTime) AS HourOfDay,
  COUNT(*) AS RideCount
FROM rides_data
GROUP BY HourOfDay
ORDER BY RideCount DESC;


# Count total rides by pickup zone to identify high-demand areas.
#  Useful for analyzing zone-wise performance and optimizing resource deployment.
SELECT 
  PickupZone, 
  COUNT(*) AS TotalRides
FROM rides_data
GROUP BY PickupZone
ORDER BY TotalRides DESC;

# Count total rides by drop-off zone to determine popular destination areas.
# Useful for identifying high-traffic zones and optimizing end-of-trip logistics.
SELECT 
  DropoffZone, 
  COUNT(*) AS TotalRides
FROM rides_data
GROUP BY  DropoffZone 
ORDER BY TotalRides DESC;


# Calculate the average fare per ride for each pickup zone.
#  Helps identify zones with higher revenue potential or pricing trends.
SELECT PickupZone, AVG(FareAmount) AS AvgFare
FROM rides_data
GROUP BY PickupZone;


# Analyze the number of cancellations by pickup zone and reason.
# Helps identify zones with higher cancellation issues and understand underlying causes.
SELECT PickupZone, CancellationReason, COUNT(*) AS Count
FROM rides_data
WHERE RideStatus LIKE 'Cancelled%'
GROUP BY PickupZone, CancellationReason
ORDER BY Count DESC;

# Count total trips between each pickup and drop-off zone pair.
# Useful for identifying the most common travel routes and optimizing zone-to-zone coverage.
SELECT 
  PickupZone, 
  DropoffZone, 
  COUNT(*) AS Trips
FROM rides_data
GROUP BY PickupZone, DropoffZone
ORDER BY Trips DESC;


# Analyze ride volume by pickup zone under different weather conditions.
# Combines temperature and rainfall status to evaluate weather impact on ride demand.
SELECT 
  r.PickupZone, 
  w.TemperatureC, 
  w.IsRaining, 
  COUNT(*) AS RideCount
FROM rides_data r
JOIN weather_data w
ON DATE(r.PickupDateTime) = w.date
GROUP BY r.PickupZone, w.TemperatureC, w.IsRaining
ORDER BY RideCount DESC;

# Compare average fare on rainy vs. non-rainy days.
# Useful for analyzing pricing trends and rider behavior in different weather conditions.
SELECT 
  w.IsRaining,
  ROUND(avg(r.FareAmount), 2) AS AvgFare
FROM rides_data r
JOIN weather_data w
  ON DATE(r.PickupDateTime) = w.date
GROUP BY w.IsRaining;

# Calculate total revenue, total rides, and average fare by pickup zone for completed rides only.
# Useful for identifying high-performing zones based on revenue generation and ride volume.
SELECT 
  PickupZone, 
  ROUND(SUM(FareAmount), 2) AS TotalRevenue,
  COUNT(*) AS TotalRides,
  ROUND(AVG(FareAmount), 2) AS AvgFare
FROM rides_data
WHERE RideStatus = 'Completed'
GROUP BY PickupZone
ORDER BY TotalRevenue DESC;

# Calculate the average delay (actual - estimated trip duration) by pickup zone.
# Only considers rides where actual duration exceeded the estimate to focus on delays.
SELECT 
  PickupZone,
  ROUND(AVG(ActualTripDurationMinutes - EstimatedTripDurationMinutes), 2) AS AvgDelay
FROM rides_data
WHERE ActualTripDurationMinutes > EstimatedTripDurationMinutes
GROUP BY PickupZone
ORDER BY AvgDelay DESC;

# Retrieve the top 10 drivers based on average rating, considering only completed rides.
# Also includes the number of rides completed to ensure ratings are based on sufficient data.
SELECT 
  DriverID,
  ROUND(AVG(DriverRating), 2) AS AvgDriverRating,
  COUNT(*) AS RidesCompleted
FROM rides_data
WHERE RideStatus = 'Completed'
GROUP BY DriverID
ORDER BY AvgDriverRating DESC
LIMIT 10;


#Count the number of cancelled rides grouped by rainy vs. non-rainy days.
# Helps analyze the impact of weather conditions on ride cancellations.
SELECT 
  w.IsRaining, 
  COUNT(*) AS CancelledRides
FROM rides_data r
JOIN weather_data w ON DATE(r.PickupDateTime) = w.date
WHERE r.RideStatus LIKE 'Cancelled%'
GROUP BY w.IsRaining;


# Analyze daily ride volume over time by extracting the date from PickupDateTime.
# Useful for identifying trends, spikes, or drops in ride demand.
SELECT DATE(PickupDateTime) AS RideDate, COUNT(*) AS RideCount
FROM rides_data
GROUP BY RideDate
ORDER BY RideDate;

# Identify the most active customers based on total ride count.
# Helps in customer segmentation, loyalty analysis, and targeting frequent riders.
SELECT CustomerID, COUNT(*) AS Rides
FROM rides_data
GROUP BY CustomerID
ORDER BY Rides DESC;


# Analyze average customer and driver ratings across different ride statuses.
# Helps identify how ride outcomes (e.g. completed, cancelled) affect satisfaction on both sides.
SELECT 
  RideStatus,
  ROUND(AVG(CustomerRating), 2) AS AvgCustRating,
  ROUND(AVG(DriverRating), 2) AS AvgDrvRating
FROM rides_data
GROUP BY RideStatus;


# Analyze ride volume and average fare across different surge multiplier levels.
# Helps understand how surge pricing impacts rider demand and fare values.
SELECT SurgeMultiplier, COUNT(*) AS RideCount, AVG(FareAmount) AS AvgFare
FROM rides_data
GROUP BY SurgeMultiplier
ORDER BY RideCount DESC;


# Analyze the impact of events on ride activity by pickup zone.
# Calculates average fare and total rides during each event day where the event impacted the pickup zone.
SELECT
  r.PickupZone,
  e.EventName,
  e.Event_Date,
   ROUND(avg(r.FareAmount), 2) AS AvgFare,
  COUNT(*) AS RideCountDuringEvent
FROM rides_data r
JOIN events_data e ON r.PickupZone = e.ImpactZone
  AND DATE(r.PickupDateTime) = e.Event_Date
GROUP BY r.PickupZone, e.EventName, e.Event_Date
ORDER BY RideCountDuringEvent DESC;


# Compare ride patterns between weekdays and weekends.
# Includes total rides, average fare, and average trip duration for each day type.
SELECT
  CASE 
    WHEN DAYOFWEEK(PickupDateTime) IN (1,7) THEN 'Weekend' 
    ELSE 'Weekday' 
  END AS DayType,
  COUNT(*) AS RideCount,
  ROUND(AVG(FareAmount), 2) AS AvgFare,
  ROUND(AVG(ActualTripDurationMinutes), 2) AS AvgTripDuration
FROM rides_data
GROUP BY DayType;
                     


SET SQL_SAFE_UPDATES = 0; -- Temporarily disable safe updates if not using a primary key in WHERE

# begining of the transaction
START TRANSACTION;

-- Update FareAmount to NULL for all cancelled rides
UPDATE rides_data
SET FareAmount = NULL
WHERE RideStatus LIKE 'Cancelled%'; -- Use LIKE 'Cancelled%' to catch any cancellation status

# commiting the chabges to the data base
COMMIT;

SET SQL_SAFE_UPDATES = 1; -- Re-enable safe updates


SELECT COUNT(*)
FROM rides_data
WHERE RideStatus LIKE 'Cancelled%' AND DistanceKM > 0;

SELECT COUNT(*)
FROM rides_data
WHERE RideStatus = 'Completed' AND  fare_per_km IS NULL;

SELECT COUNT(*)
FROM rides_data
WHERE DropoffDateTime < PickupDateTime;

SELECT COUNT(*)
FROM rides_data
WHERE RideStatus = 'Completed' AND DistanceKM = 0;




SET SQL_SAFE_UPDATES = 0; -- Temporarily disable safe updates if not using a primary key in WHERE

# begining the transaction
START TRANSACTION;

-- Update DistanceKM to NULL for completed rides that currently show 0 KM
UPDATE rides_data
SET DistanceKM = NULL
WHERE RideStatus = 'Completed' AND DistanceKM = 0;

# commiting the chabges to the data base
COMMIT;

SET SQL_SAFE_UPDATES = 1; -- Re-enable safe updates


#Identify completed rides that have a missing distance value but a valid fare.
# These records are inconsistent and may skew fare-per-kilometer or performance metrics.
    SELECT COUNT(*)
FROM rides_data
WHERE RideStatus = 'Completed'
  AND DistanceKM IS NULL
  AND FareAmount IS NOT NULL;
  
  
set sql_safe_updates=0;
  
 # Delete logically invalid records: completed rides with no distance but with fare — likely data errors.
  DELETE FROM rides_data
WHERE RideStatus = 'Completed'
  AND DistanceKM is NULL
  AND FareAmount IS NOT NULL;
  
  # Review current state of the cleaned rides_data table.
select * from rides_data;

#Review the enriched_rides view to validate downstream impact of the deletion.
select * from enriched_rides;

#Count how many enriched rides have a null fare_per_km value but were cancelled by the customer.
# This is expected since cancelled rides may have 0 distance and 0 or null fare.
select count(*) from enriched_rides
where fare_per_km is null and ridestatus ='Cancelled by Customer';

#Count the number of records in enriched_rides where average speed could not be calculated.
# This typically occurs when ActualTripDurationMinutes is 0 or null, or when DistanceKM is missing.
select count(*) from enriched_rides
where average_speed_kmh is null;


# Create an enriched view combining ride, weather, and event data with derived metrics
CREATE OR REPLACE VIEW enriched_rides AS
 #Core ride identifiers and metadata
SELECT
    r.RideID,
    r.CustomerID,
    r.DriverID,
    r.PickupDateTime,
    r.DropoffDateTime,
    r.PickupZone,
    r.PickupLatitude,
    r.PickupLongitude,
    r.DropoffZone,
    r.DropoffLatitude,
    r.DropoffLongitude,
    r.DistanceKM,
   # Normalize FareAmount: set to 0 for cancelled rides
    CASE
        WHEN r.RideStatus = 'Cancelled by Customer' THEN 0
        ELSE r.FareAmount
    END AS FareAmount,
    r.SurgeMultiplier,
    r.RideStatus,
     #Clean up CancellationReason: set to "Not Applicable" for completed rides
    CASE
    WHEN r.RideStatus = 'Completed' THEN 'Not Applicable'
    ELSE COALESCE(r.CancellationReason, 'Not Applicable')
END AS CancellationReason,
    r.WaitingTimeMinutes,
    r.EstimatedTripDurationMinutes,
    r.ActualTripDurationMinutes,
    r.DriverRating,
    r.CustomerRating,

    -- Time-based features
    DATE(r.PickupDateTime) AS ride_date,
    HOUR(r.PickupDateTime) AS hour_of_day,
    DAYOFWEEK(r.PickupDateTime) AS day_of_week_num,
    DAYNAME(r.PickupDateTime) AS day_of_week_name,
    #Categorize into weekend/weekday
    CASE
        WHEN DAYOFWEEK(r.PickupDateTime) IN (1, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,
   # Rush hour flag (morning and evening)
    CASE
        WHEN HOUR(r.PickupDateTime) BETWEEN 8 AND 10 OR HOUR(r.PickupDateTime) BETWEEN 17 AND 20 THEN TRUE
        ELSE FALSE
    END AS is_rush_hour,
     # Additional time dimensions
    MONTH(r.PickupDateTime) AS month_of_year,
    QUARTER(r.PickupDateTime) AS quarter_of_year,
    YEAR(r.PickupDateTime) AS year_of_ride,

    -- Derived financial/efficiency metrics
    round(COALESCE(r.FareAmount / NULLIF(r.DistanceKM, 0), 0),2) AS fare_per_km,
    round((r.DistanceKM / NULLIF(r.ActualTripDurationMinutes / 60.0, 0)),2) AS average_speed_kmh,
    (r.ActualTripDurationMinutes - r.EstimatedTripDurationMinutes) AS duration_deviation_minutes,

    -- Weather (LEFT JOIN)
    w.TemperatureC,
    w.IsRaining,

    -- Event Logic
    CASE WHEN e.EventName IS NOT NULL THEN TRUE ELSE FALSE END AS is_event_day,

    -- Replace NULLs for non-event days
    CASE
        WHEN e.EventName IS NOT NULL THEN e.EventName
        ELSE 'No Event'
    END AS active_event_name,

    CASE
        WHEN e.EventName IS NOT NULL THEN e.ImpactZone
        ELSE 'No Zone'
    END AS event_impact_zone,

    CASE
        WHEN e.EventName IS NOT NULL THEN e.ImpactRadiusKM
        ELSE 0
    END AS event_impact_radius_km
# Join with weather data by ride date
FROM
    rides_data r
    
# Join with weather data by ride date    
LEFT JOIN
    weather_data w ON DATE(r.PickupDateTime) = w.Date
#Join with events that occurred on the same day and in the impacted zone (or citywide)
LEFT JOIN
    events_data e ON DATE(r.PickupDateTime) = e.event_date
                 AND (r.PickupZone = e.ImpactZone OR e.ImpactZone = 'Citywide');
                 
                 
# View all records in the enriched_rides view to explore the full dataset.
#  This includes raw, derived, weather, and event-related features.
select * from enriched_rides;

# Check for data quality issues: completed rides with null fare amounts.
# These are invalid and may affect fare-based calculations or KPIs.
select count(*) from enriched_rides
where fareamount is null and ridestatus = 'completed';

