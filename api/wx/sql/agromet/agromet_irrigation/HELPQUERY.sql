WITH cte AS (
    SELECT
        st.id
        ,st.name AS station_name
        ,EXTRACT(YEAR FROM ds.day) AS year
        ,COUNT(*) FILTER(WHERE vr.symbol = 'TEMPMIN') AS "TEMPMIN"
        ,COUNT(*) FILTER(WHERE vr.symbol = 'TEMPMAX') AS "TEMPMAX"
        ,COUNT(*) FILTER(WHERE vr.symbol = 'PRECIP') AS "PRECIP"
    FROM daily_summary ds
    JOIN wx_station st ON ds.station_id = st.id
    JOIN wx_variable vr ON ds.variable_id = vr.id
    WHERE vr.symbol IN ('TEMPMIN', 'PRECIP', 'TEMPMAX') AND st.is_automatic = FALSE
    GROUP BY st.id, st.name, EXTRACT(YEAR FROM ds.day) 
)
SELECT * FROM cte
WHERE "TEMPMIN" > 365 AND "TEMPMAX" > 365 AND "PRECIP" > 365
ORDER BY year DESC;

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------

WITH cte AS (
    SELECT
        st.id
        ,st.name AS station_name
        ,EXTRACT(YEAR FROM ds.day) AS year
        ,COUNT(*) FILTER(WHERE vr.symbol = 'TEMP') AS "TEMPMIN"
        ,COUNT(*) FILTER(WHERE vr.symbol = 'TEMP') AS "TEMPMAX"
        ,COUNT(*) FILTER(WHERE vr.symbol = 'PRECIP') AS "PRECIP"
        ,COUNT(*) FILTER(WHERE vr.symbol = 'PRESSTN') AS "PRESSTN"
        ,COUNT(*) FILTER(WHERE vr.symbol = 'WNDSPAVG') AS "WNDSPAVG"
        ,COUNT(*) FILTER(WHERE vr.symbol = 'SOLARRAD') AS "SOLARRAD"
        ,COUNT(*) FILTER(WHERE vr.symbol = 'RH') AS "RH"
    FROM daily_summary ds
    JOIN wx_station st ON ds.station_id = st.id
    JOIN wx_variable vr ON ds.variable_id = vr.id
    WHERE vr.symbol IN ('TEMP', 'PRECIP', 'PRESSTN', 'WNDSPAVG', 'SOLARRAD', 'RH')
    GROUP BY st.id, st.name, EXTRACT(YEAR FROM ds.day)
)
SELECT *
FROM cte
WHERE LEAST("TEMPMIN", "TEMPMAX", "PRECIP", "PRESSTN", "WNDSPAVG", "SOLARRAD", "RH") > 364
ORDER BY year;

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------

SELECT
    st.id
    ,st.name AS station_name
    ,vr.symbol
    ,ys.year
    ,ys.num_records
FROM yearly_summary ys
JOIN wx_station st ON ys.station_id = st.id
JOIN wx_variable vr ON ys.variable_id = vr.id
WHERE st.id=4 AND ys.year=2020 AND vr.symbol IN ('TEMP', 'PRECIP', 'PRESSTN', 'WNDSPAVG', 'SOLARRAD', 'RH')

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------

WITH daily_data AS (
    SELECT
        day,
        station_id,
        variable_id,
        min_value,
        max_value,
        avg_value,
        sum_value,
        num_records
    FROM daily_summary ds
    JOIN wx_variable vr ON ds.variable_id = vr.id
    WHERE vr.symbol IN ('TEMP', 'PRECIP', 'PRESSTN', 'WNDSPAVG', 'SOLARRAD', 'RH')
      AND ds.station_id = 123
      AND day >= '2024-01-01'
      AND day <= '2024-12-31'
),
year_intervals AS (
    SELECT generate_series(-16, 0) AS year_offset
)
INSERT INTO daily_summary (
    created_at,
    updated_at,
    day,
    station_id,
    variable_id,
    min_value,
    max_value,
    avg_value,
    sum_value,
    num_records
)
SELECT
    NOW(),
    NOW(),
    (day + (year_offset || ' years')::interval)::date AS day,
    225 AS station_id,
    variable_id,
    min_value,
    max_value,
    avg_value,
    sum_value,
    num_records
FROM daily_data
JOIN year_intervals ON TRUE
ON CONFLICT (day, station_id, variable_id) 
DO NOTHING;

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------

WITH daily_data AS (
    SELECT
        day,
        station_id,
        variable_id,
        min_value,
        max_value,
        avg_value,
        sum_value,
        num_records
    FROM daily_summary ds
    JOIN wx_variable vr ON ds.variable_id = vr.id
    WHERE vr.symbol IN ('TEMPMIN', 'TEMPMAX', 'PRECIP')
      AND ds.station_id = 14
      AND day >= '2008-01-01'
      AND day <= '2008-12-31'
),
year_intervals AS (
    SELECT generate_series(0, 16) AS year_offset
)
INSERT INTO daily_summary (
    created_at,
    updated_at,
    day,
    station_id,
    variable_id,
    min_value,
    max_value,
    avg_value,
    sum_value,
    num_records
)
SELECT
    NOW(),
    NOW(),
    (day + (year_offset || ' years')::interval)::date AS day,
    224 AS station_id,
    variable_id,
    min_value,
    max_value,
    avg_value,
    sum_value,
    num_records
FROM daily_data
JOIN year_intervals ON TRUE
ON CONFLICT (day, station_id, variable_id) 
DO NOTHING;




--------------------------------------------------------------------------------
WITH daily_data AS (
    SELECT
        day,
        station_id,
        variable_id,
        min_value,
        max_value,
        avg_value,
        sum_value,
        num_records
    FROM daily_summary ds
    JOIN wx_variable vr ON ds.variable_id = vr.id
    WHERE vr.symbol IN ('TEMPMIN', 'TEMPMAX', 'PRECIP')
      AND ds.station_id = 14
      AND day >= '2008-01-01'
      AND day <= '2008-11-13'
),
year_intervals AS (
    SELECT generate_series(17, 17) AS year_offset
)
INSERT INTO daily_summary (
    created_at,
    updated_at,
    day,
    station_id,
    variable_id,
    min_value,
    max_value,
    avg_value,
    sum_value,
    num_records
)
SELECT
    NOW(),
    NOW(),
    (day + (year_offset || ' years')::interval)::date AS day,
    224 AS station_id,
    variable_id,
    min_value,
    max_value,
    avg_value,
    sum_value,
    num_records
FROM daily_data
JOIN year_intervals ON TRUE
ON CONFLICT (day, station_id, variable_id) 
DO NOTHING;



WITH daily_data AS (
    SELECT
        day,
        station_id,
        variable_id,
        min_value,
        max_value,
        avg_value,
        sum_value,
        num_records
    FROM daily_summary ds
    JOIN wx_variable vr ON ds.variable_id = vr.id
    WHERE vr.symbol IN ('TEMP', 'PRECIP', 'PRESSTN', 'WNDSPAVG', 'SOLARRAD', 'RH')
      AND ds.station_id = 123
      AND day >= '2024-01-01'
      AND day <= '2024-11-13'
),
year_intervals AS (
    SELECT generate_series(1, 1) AS year_offset
)
INSERT INTO daily_summary (
    created_at,
    updated_at,
    day,
    station_id,
    variable_id,
    min_value,
    max_value,
    avg_value,
    sum_value,
    num_records
)
SELECT
    NOW(),
    NOW(),
    (day + (year_offset || ' years')::interval)::date AS day,
    225 AS station_id,
    variable_id,
    min_value,
    max_value,
    avg_value,
    sum_value,
    num_records
FROM daily_data
JOIN year_intervals ON TRUE
ON CONFLICT (day, station_id, variable_id) 
DO NOTHING;
