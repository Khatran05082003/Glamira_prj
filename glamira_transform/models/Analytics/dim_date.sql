WITH dim_date AS (
    SELECT DISTINCT
        CAST(TIMESTAMP_SECONDS(CAST(time_stamp AS INT64)) AS DATE) AS date,
        CASE 
            WHEN EXTRACT(DAYOFWEEK FROM TIMESTAMP_SECONDS(CAST(time_stamp AS INT64))) = 1 THEN 'Sunday'
            WHEN EXTRACT(DAYOFWEEK FROM TIMESTAMP_SECONDS(CAST(time_stamp AS INT64))) = 2 THEN 'Monday'
            WHEN EXTRACT(DAYOFWEEK FROM TIMESTAMP_SECONDS(CAST(time_stamp AS INT64))) = 3 THEN 'Tuesday'
            WHEN EXTRACT(DAYOFWEEK FROM TIMESTAMP_SECONDS(CAST(time_stamp AS INT64))) = 4 THEN 'Wednesday'
            WHEN EXTRACT(DAYOFWEEK FROM TIMESTAMP_SECONDS(CAST(time_stamp AS INT64))) = 5 THEN 'Thursday'
            WHEN EXTRACT(DAYOFWEEK FROM TIMESTAMP_SECONDS(CAST(time_stamp AS INT64))) = 6 THEN 'Friday'
            ELSE 'Saturday'
        END AS day_of_week,
        CASE 
            WHEN EXTRACT(DAYOFWEEK FROM TIMESTAMP_SECONDS(CAST(time_stamp AS INT64))) = 1 THEN 'Sun'
            WHEN EXTRACT(DAYOFWEEK FROM TIMESTAMP_SECONDS(CAST(time_stamp AS INT64))) = 2 THEN 'Mon'
            WHEN EXTRACT(DAYOFWEEK FROM TIMESTAMP_SECONDS(CAST(time_stamp AS INT64))) = 3 THEN 'Tue'
            WHEN EXTRACT(DAYOFWEEK FROM TIMESTAMP_SECONDS(CAST(time_stamp AS INT64))) = 4 THEN 'Wed'
            WHEN EXTRACT(DAYOFWEEK FROM TIMESTAMP_SECONDS(CAST(time_stamp AS INT64))) = 5 THEN 'Thu'
            WHEN EXTRACT(DAYOFWEEK FROM TIMESTAMP_SECONDS(CAST(time_stamp AS INT64))) = 6 THEN 'Fri'
            ELSE 'Sat'
        END AS day_of_week_short,
        CASE 
            WHEN EXTRACT(DAYOFWEEK FROM TIMESTAMP_SECONDS(CAST(time_stamp AS INT64))) IN (1, 7) THEN 'Weekend'
            ELSE 'Weekday'
        END AS is_weekday_or_weekend,
        FORMAT_TIMESTAMP('%d', TIMESTAMP_SECONDS(CAST(time_stamp AS INT64))) AS day_of_month,
        FORMAT_TIMESTAMP('%Y-%m', TIMESTAMP_SECONDS(CAST(time_stamp AS INT64))) AS year_month,
        EXTRACT(MONTH FROM TIMESTAMP_SECONDS(CAST(time_stamp AS INT64))) AS month,
        EXTRACT(DAYOFYEAR FROM TIMESTAMP_SECONDS(CAST(time_stamp AS INT64))) AS day_of_the_year,
        FORMAT_TIMESTAMP('%W', TIMESTAMP_SECONDS(CAST(time_stamp AS INT64))) AS week_of_year,
        EXTRACT(QUARTER FROM TIMESTAMP_SECONDS(CAST(time_stamp AS INT64))) AS quarter_number,
        FORMAT_TIMESTAMP('%Y', TIMESTAMP_SECONDS(CAST(time_stamp AS INT64))) AS year,
        EXTRACT(YEAR FROM TIMESTAMP_SECONDS(CAST(time_stamp AS INT64))) AS year_number
    FROM {{ ref('stg_raw') }}
    WHERE SAFE_CAST(time_stamp AS INT64) IS NOT NULL
)

SELECT * 
FROM dim_date

