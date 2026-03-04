-- Validate remapped stations (original source station_id = 0) by station name
WITH start_station_name_id_map AS (
  SELECT
    start_station_name AS name,
    MIN(CAST(start_station_id AS STRING)) AS mapped_station_id
  FROM `automatic-ace-488412-a7.austin_bikeshare_demo.fact_trips`
  WHERE start_station_name IS NOT NULL
    AND start_station_id IS NOT NULL
    AND CAST(start_station_id AS STRING) != '0'
  GROUP BY start_station_name
  HAVING COUNT(DISTINCT CAST(start_station_id AS STRING)) = 1
),
end_station_name_id_map AS (
  SELECT
    end_station_name AS name,
    MIN(CAST(end_station_id AS STRING)) AS mapped_station_id
  FROM `automatic-ace-488412-a7.austin_bikeshare_demo.fact_trips`
  WHERE end_station_name IS NOT NULL
    AND end_station_id IS NOT NULL
    AND CAST(end_station_id AS STRING) != '0'
  GROUP BY end_station_name
  HAVING COUNT(DISTINCT CAST(end_station_id AS STRING)) = 1
),
remapped_source AS (
  SELECT
    bs.name,
    COALESCE(ss.mapped_station_id, es.mapped_station_id) AS mapped_station_id
  FROM `bigquery-public-data.austin_bikeshare.bikeshare_stations` bs
  LEFT JOIN start_station_name_id_map ss ON bs.name = ss.name
  LEFT JOIN end_station_name_id_map es ON bs.name = es.name
  WHERE CAST(bs.station_id AS STRING) = '0'
    AND COALESCE(ss.mapped_station_id, es.mapped_station_id) IS NOT NULL
),
expected AS (
  SELECT
    r.name,
    r.mapped_station_id AS station_id,
    COALESCE(SUM(CASE
      WHEN CAST(t.start_station_id AS STRING) = r.mapped_station_id
      THEN t.duration_minutes * 60 ELSE 0
    END), 0) AS exp_total_duration_seconds,
    COALESCE(SUM(CASE
      WHEN CAST(t.start_station_id AS STRING) = r.mapped_station_id
       AND t.start_station_name IS NOT NULL
      THEN 1 ELSE 0
    END), 0) AS exp_total_starts,
    COALESCE(SUM(CASE
      WHEN CAST(t.end_station_id AS STRING) = r.mapped_station_id
       AND t.end_station_name IS NOT NULL
      THEN 1 ELSE 0
    END), 0) AS exp_total_ends
  FROM remapped_source r
  CROSS JOIN `automatic-ace-488412-a7.austin_bikeshare_demo.fact_trips` t
  GROUP BY r.name, r.mapped_station_id
),
actual AS (
  SELECT
    d.name,
    d.station_id,
    d.total_duration_seconds,
    d.total_starts,
    d.total_ends
  FROM `automatic-ace-488412-a7.austin_bikeshare_demo_star.dim_station` d
  JOIN remapped_source r
    ON d.name = r.name
   AND d.station_id = r.mapped_station_id
)
SELECT
  a.name,
  a.station_id,
  a.total_duration_seconds, e.exp_total_duration_seconds,
  a.total_starts,           e.exp_total_starts,
  a.total_ends,             e.exp_total_ends,
  (a.total_duration_seconds = e.exp_total_duration_seconds
   AND a.total_starts = e.exp_total_starts
   AND a.total_ends = e.exp_total_ends) AS is_match
FROM actual a
JOIN expected e
  ON a.name = e.name
 AND a.station_id = e.station_id
ORDER BY is_match, a.name;