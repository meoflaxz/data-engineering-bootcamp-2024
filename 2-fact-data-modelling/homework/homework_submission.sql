
--Deduplication of game_details table
WITH deduped AS (
    SELECT
        *, ROW_NUMBER() OVER (PARTITION BY game_id, team_id, player_id) AS row_num
    FROM game_details
)
SELECT
    game_id,
    team_id,
    player_id,
    start_position,
    comment,
    min,
    fgm,
    oreb,
    dreb,
    reb,
    ast,
    stl,
    blk,
    "TO" AS to,
    pf,
    pts,
    plus_minus
FROM deduped
--  take only one copy of each PARTITION
-- removing duplicates
WHERE row_num = 1;

--------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------

-- A DDL for an user_devices_cumulated
CREATE TABLE user_devices_cumulated (
    user_id NUMERIC,
    browser_type TEXT,
    device_activity_datelist DATE[],
    date DATE,
    PRIMARY KEY (user_id, browser_type, date)
);
-- CUMULATIVE QUERY TO GENERATE device_activity_datelist from events table

--------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------

INSERT INTO user_devices_cumulated 
WITH yesterday AS (
    SELECT * FROM user_devices_cumulated
    WHERE date = DATE('2023-01-01')
    --2022-12-31
),
today AS (
    -- removing deduplication that may occur to fit the primary key of user_id, browser_type, and date
    WITH deduped AS (
    SELECT 
        e.user_id,
        d.browser_type,
        CAST(e.event_time AS DATE) AS date_active,
        ROW_NUMBER() OVER (PARTITION BY e.user_id, d.browser_type, CAST(e.event_time AS DATE)) AS row_num
    FROM events e 
    LEFT JOIN devices d
        ON e.device_id = d.device_id
    WHERE CAST(e.event_time AS DATE) = DATE('2023-01-02')
        AND user_id IS NOT NULL
        AND browser_type IS NOT NULL
    GROUP BY e.user_id, d.browser_type, e.event_time
    )
    -- take only one version of each 
    SELECT
        user_id,
        browser_type,
        DATE(date_active) AS date_active
    FROM deduped
        WHERE row_num = 1
)
SELECT
    COALESCE(t.user_id, y.user_id) AS user_id,
    COALESCE(t.browser_type, y.browser_type) AS browser_type,
    -- if yesterday data is null, insert new data from today
    CASE
        WHEN y.device_activity_datelist IS NULL THEN ARRAY[t.date_active]
    -- if no record for new incoming data, user previous data, if not concat new data to yesterday data
        WHEN t.date_active IS NULL THEN y.device_activity_datelist
            ELSE ARRAY[t.date_active] || y.device_activity_datelist
    END::DATE[] AS activity_datelist,
    -- yesterday date is plus one to align with today's date
    COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date
FROM today t
FULL OUTER JOIN yesterday y
ON t.user_id = y.user_id
AND t.browser_type = y.browser_type
-- if insert conflict for primary key, just add the new datelist
ON CONFLICT (user_id, browser_type, date)
DO UPDATE SET
   device_activity_datelist = EXCLUDED.device_activity_datelist;


--------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------

-- DATELIST CONVERSION

WITH users AS (
    -- all cumulative data until this record
    -- depends on cumulative table,if no data is createa for the specific date, nothing is shown
    -- important to take the latest date according to cumulative table if you want all data
    SELECT * FROM user_devices_cumulated
    WHERE date = DATE('2023-01-09')
),
series AS (
    -- generate whole month array to be later used as binary representation step
    SELECT
        CAST(series_date AS DATE) AS series_date
    FROM generate_series(   DATE('2023-01-01'),
                            DATE('2023-01-31'),
                            INTERVAL '1 day') AS series_date
),
place_holder_ints AS (
    SELECT
        CASE
        -- compare if the dates_active do exist in the date array using the @> operator
            WHEN device_activity_datelist @> ARRAY[series_date::DATE]
            -- convert into the binary representation using the POW function 
            -- cast them to BIGINT format because the number is very huge
            THEN CAST(POW(2, 32 - (date - DATE(series_date))) AS BIGINT)
            ELSE 0
        END AS placeholder_int_value, *
    FROM users
    CROSS JOIN series
)
SELECT
    user_id,
    CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)),
    -- using the BIT_COUNT function to retrieve them back as normal value
    -- whole month representation
    BIT_COUNT(CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 AS dim_is_monthly_active,
    -- 1111111 following a week
    BIT_COUNT(CAST('11111110000000000000000000000000' AS BIT(32)) & CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 AS dim_is_weekly_active,
    -- 1111111 following a day
    BIT_COUNT(CAST('10000000000000000000000000000000' AS BIT(32)) & CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 AS dim_is_daily_active
FROM place_holder_ints
GROUP BY user_id;

--------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------

-- DDL for hosts_cumulated table

CREATE TABLE hosts_cumulated (
    host TEXT,
    host_activity_datelist DATE[],
    date DATE,
    PRIMARY KEY (host, date)
);

-- ANALYSE MIN AND MAX FOR EVENT TIME TO GENERATE MONTHLY DATA
-- MIN -> 2023-01-01
-- MAX -> 2023-01-31
SELECT  
    MIN(event_time) AS min_event_time,
    MAX(event_time) AS max_event_time
FROM events;

--------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------

-- INCREMENTAL QUERY TO GENERATE host_activity_datelist

INSERT INTO hosts_cumulated
WITH yesterday AS (
    SELECT * FROM hosts_cumulated
    WHERE date = DATE('2023-01-30')
),
today AS (
    WITH deduped AS (
        SELECT
            host,
            DATE(CAST(event_time AS DATE)) AS date_active,
            ROW_NUMBER() OVER(PARTITION BY host, DATE(CAST(event_time AS DATE))) as row_num
        FROM events
        WHERE DATE(CAST(event_time AS DATE)) = DATE('2023-01-31')
            AND host IS NOT NULL
        GROUP BY host, DATE(CAST(event_time AS DATE))
    )
    SELECT * FROM deduped
    WHERE row_num = 1
)
SELECT
    COALESCE(t.host, y.host) AS host,
    CASE
        WHEN y.host_activity_datelist IS NULL THEN ARRAY[t.date_active]
        WHEN t.date_active IS NULL THEN y.host_activity_datelist
            ELSE ARRAY[t.date_active] || y.host_activity_datelist
    END::DATE[] AS host_activity_datelist,
    COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date
FROM today t
FULL OUTER JOIN yesterday y
ON t.host = y.host;

-- SHOW TABLE
SELECT * FROM hosts_cumulated;