--How many total crimes were recorded in London each year?
select year,format("%'d",sum(value)) as total_crimes from bigquery-public-data.london_crime.crime_by_lsoa
group by year
order by  sum(value) desc;

--Which year had the highest number of total crimes?
select year,format("%'d",sum(value)) as total_crimes from bigquery-public-data.london_crime.crime_by_lsoa
group by year
order by  sum(value) desc
limit 1;

--What are the top 10 boroughs by total crime count?
select borough,format("%'d",sum(value)) as total_crimes
from bigquery-public-data.london_crime.crime_by_lsoa
group by borough
order by sum(value) desc
limit 10;

--Which month tends to have the highest crime rate overall?
select year,month,format("%'d",sum(value)) as total_crimes
from bigquery-public-data.london_crime.crime_by_lsoa
group by year,month
order by sum(value) desc
limit 1
;

--How many unique boroughs  are there in the dataset?
select count(distinct borough) as unique_boroughs
from bigquery-public-data.london_crime.crime_by_lsoa;

--What are the most common major crime categories?
select major_category,format("%'d",sum(value)) as total_crimes
from bigquery-public-data.london_crime.crime_by_lsoa
group  by   major_category
order by sum(value) desc
limit 5;

--Which borough has the lowest number of recorded crimes?
select borough,format("%'d",sum(value)) as total_crimes
from bigquery-public-data.london_crime.crime_by_lsoa
group by borough
order by sum(value)
limit 1;

--What percentage of total crimes come from the “Theft and Handling” category?
select 'Theft and Handling', format("%'d",sum(case when major_category='Theft and Handling' then value else 0 end)) as thef_count,format("%'d",sum(value)) as total_crimes,round(sum(case when major_category='Theft and Handling' then value else 0 end)*100/sum(value),2) as crime_pct
from bigquery-public-data.london_crime.crime_by_lsoa;

--What’s the trend of “Violence Against the Person” crimes over time?
select  year,format("%'d",sum(value)) as total_crimes
from bigquery-public-data.london_crime.crime_by_lsoa
where major_category='Violence Against the Person'
group by year
order by sum(value) desc;

--What is the total number of crimes recorded in 2016?
select year,format("%'d",sum(value)) as total_crimes
from bigquery-public-data.london_crime.crime_by_lsoa
where year=2016
group by year;


--What are the top 5 boroughs for each major crime category?
with total_crimes as(
  select borough,
  major_category,
  format("%'d",sum(value)) as total_crime
from bigquery-public-data.london_crime.crime_by_lsoa
group by borough,major_category
),
RankedCrimes AS (
  
  SELECT
    borough,
    major_category,
    total_crime,
    RANK() OVER (PARTITION BY major_category ORDER BY total_crime DESC) AS crime_rank
  FROM
    total_crimes
)

SELECT
  major_category,
  borough,
  total_crime
FROM
  RankedCrimes
WHERE
  crime_rank <= 5
ORDER BY
  major_category,
  crime_rank;

--How does crime distribution change across years for the “Burglary” category?
select year,sum(value)as total_crimes 
from bigquery-public-data.london_crime.crime_by_lsoa
where major_category= 'Burglary'
group by year
order by year;


--Which boroughs show the highest increase in crime from 2010 to 2016?
WITH borough_year AS (
  SELECT
    borough,
    year,
    SUM(value) AS total_crimes
  FROM `bigquery-public-data.london_crime.crime_by_lsoa`
  WHERE year IN (2010, 2016)
  GROUP BY borough, year
),

borough_pivot AS (
  SELECT
    borough,
    COALESCE(MAX(CASE WHEN year = 2010 THEN total_crimes END), 0) AS crimes_2010,
    COALESCE(MAX(CASE WHEN year = 2016 THEN total_crimes END), 0) AS crimes_2016
  FROM borough_year
  GROUP BY borough
)

SELECT
  borough,
  crimes_2010,
  crimes_2016,
  crimes_2016 - crimes_2010 AS absolute_increase,
  ROUND( SAFE_DIVIDE(crimes_2016 - crimes_2010, NULLIF(crimes_2010,0)) * 100, 2) AS pct_change_from_2010
FROM borough_pivot
ORDER BY absolute_increase DESC
LIMIT 10;

--Which crime types are increasing or decreasing over time?
WITH yearly_crimes AS (
  SELECT
    major_category,
    year,
    SUM(value) AS total_crimes
  FROM `bigquery-public-data.london_crime.crime_by_lsoa`
  GROUP BY major_category, year
),

change_over_time AS (
  SELECT
    major_category,
    MIN(total_crimes) AS min_crimes,
    MAX(total_crimes) AS max_crimes,
    MAX(year) - MIN(year) AS years_covered,
    SUM(total_crimes) AS total_crimes_all_years,
    ROUND(SAFE_DIVIDE(MAX(total_crimes) - MIN(total_crimes), MIN(total_crimes)) * 100, 2) AS pct_change
  FROM yearly_crimes
  GROUP BY major_category
)

SELECT
  major_category,
  min_crimes,
  max_crimes,
  pct_change,
  CASE
    WHEN pct_change > 0 THEN 'Increasing'
    WHEN pct_change < 0 THEN 'Decreasing'
    ELSE 'No Change'
  END AS trend
FROM change_over_time
ORDER BY pct_change DESC;



--What are the top 3 minor crime categories in 2016?
select minor_category,format("%'d",sum(value)) as total_crimes
from bigquery-public-data.london_crime.crime_by_lsoa
where year=2016
group by minor_category
order by sum(value) desc
limit 3;


--What are the top 5 safest boroughs (lowest crime per year)?
select borough,year,sum(value)as total_crimes
from bigquery-public-data.london_crime.crime_by_lsoa
group by  borough,year
order by  sum(value)
limit 5;

--What percentage of crimes are Violence vs. Theft in 2015?
WITH crime_totals AS (
  SELECT
    major_category,
    SUM(value) AS total_crimes
  FROM `bigquery-public-data.london_crime.crime_by_lsoa`
  WHERE year = 2015
    AND major_category IN ('Violence Against the Person', 'Theft and Handling')
  GROUP BY major_category
),
overall_total AS (
  SELECT SUM(total_crimes) AS all_crimes FROM crime_totals
)

SELECT
  c.major_category,
  c.total_crimes,
  ROUND(SAFE_DIVIDE(c.total_crimes, o.all_crimes) * 100, 2) AS percentage_share
FROM crime_totals c, overall_total o
ORDER BY percentage_share DESC;


--How does crime vary seasonally (e.g., summer vs. winter months)?
WITH month_crime AS (
  SELECT
    year,
    month,
    SUM(value) AS total_crimes
  FROM `bigquery-public-data.london_crime.crime_by_lsoa`
  GROUP BY year, month
),

season_tagged AS (
  SELECT
    year,
    month,
    total_crimes,
    CASE 
      WHEN month IN (12, 1, 2) THEN 'Winter'
      WHEN month IN (3, 4, 5) THEN 'Spring'
      WHEN month IN (6, 7, 8) THEN 'Summer'
      WHEN month IN (9, 10, 11) THEN 'Autumn'
    END AS season
  FROM month_crime
),

season_totals AS (
  SELECT
    season,
    SUM(total_crimes) AS total_crimes_per_season
  FROM season_tagged
  GROUP BY season
),

overall_total AS (
  SELECT SUM(total_crimes_per_season) AS total_crimes_all_seasons
  FROM season_totals
)

SELECT
  s.season,
  s.total_crimes_per_season,
  ROUND(SAFE_DIVIDE(s.total_crimes_per_season, o.total_crimes_all_seasons) * 100, 2) AS pct_of_total
FROM season_totals s, overall_total o
ORDER BY pct_of_total DESC;



--Which borough had the most Theft-related crimes in 2016?
select borough,sum(value) as total_crimes
from bigquery-public-data.london_crime.crime_by_lsoa
where major_category ='Theft and Handling' and year =2016
group by borough
order by sum(value) desc
limit 1;

--Which borough had the largest year-on-year decrease in total crime?
WITH borough_year AS (
  SELECT
    borough,
    year,
    SUM(value) AS total_crimes
  FROM `bigquery-public-data.london_crime.crime_by_lsoa`
  GROUP BY borough, year
),

borough_lag AS (
  SELECT
    borough,
    year,
    total_crimes,
    LAG(total_crimes) OVER(PARTITION BY borough ORDER BY year) AS prev_year_crimes
  FROM borough_year
),

borough_change AS (
  SELECT
    borough,
    year,
    total_crimes,
    prev_year_crimes,
    total_crimes - prev_year_crimes AS change_in_crimes
  FROM borough_lag
  WHERE prev_year_crimes IS NOT NULL
)

SELECT
  borough,
  year,
  prev_year_crimes,
  total_crimes,
  change_in_crimes,
  ROUND(SAFE_DIVIDE(change_in_crimes, prev_year_crimes) * 100, 2) AS pct_change
FROM borough_change
ORDER BY change_in_crimes ASC
LIMIT 1;
