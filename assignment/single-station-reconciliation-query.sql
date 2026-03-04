WITH params AS (
  SELECT '3381' AS station_id
),
filtered_trips AS (
  SELECT
    CAST(start_station_id AS STRING) AS start_station_id,
    CAST(end_station_id AS STRING) AS end_station_id,
    start_station_name,
    end_station_name,
    duration_minutes
  FROM `automatic-ace-488412-a7.austin_bikeshare_demo.fact_trips` t
  CROSS JOIN params p
  WHERE CAST(t.start_station_id AS STRING) = p.station_id
     OR CAST(t.end_station_id AS STRING) = p.station_id
),
expected AS (
  SELECT
    p.station_id,
    COALESCE(SUM(CASE
      WHEN f.start_station_id = p.station_id
      THEN f.duration_minutes * 60 ELSE 0
    END), 0) AS exp_total_duration_seconds,
    COALESCE(SUM(CASE
      WHEN f.start_station_id = p.station_id
       AND f.start_station_name IS NOT NULL
      THEN 1 ELSE 0
    END), 0) AS exp_total_starts,
    COALESCE(SUM(CASE
      WHEN f.end_station_id = p.station_id
       AND f.end_station_name IS NOT NULL
      THEN 1 ELSE 0
    END), 0) AS exp_total_ends
  FROM filtered_trips f
  CROSS JOIN params p
  GROUP BY p.station_id
),
actual AS (
  SELECT
    d.station_id,
    d.total_duration_seconds,
    d.total_starts,
    d.total_ends
  FROM `automatic-ace-488412-a7.austin_bikeshare_demo_star.dim_station` d
  JOIN params p ON d.station_id = p.station_id
)
SELECT
  a.station_id,
  a.total_duration_seconds, e.exp_total_duration_seconds,
  a.total_starts,           e.exp_total_starts,
  a.total_ends,             e.exp_total_ends,
  (a.total_duration_seconds = e.exp_total_duration_seconds
   AND a.total_starts = e.exp_total_starts
   AND a.total_ends = e.exp_total_ends) AS is_match
FROM actual a
JOIN expected e USING (station_id);