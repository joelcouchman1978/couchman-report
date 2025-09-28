COPY (
  WITH base AS (
    SELECT
      COALESCE(player_name,'') AS player_name,
      COALESCE(minutes_played,0.0) AS minutes,
      COALESCE(tackles,0.0) AS tackles,
      COALESCE(missed_tackles,0.0) AS missed_tackles,
      COALESCE(run_metres,0.0) AS run_metres,
      COALESCE(post_contact_metres,0.0) AS post_contact_metres,
      COALESCE(penalties_conceded,0.0) AS penalties_conceded,
      COALESCE(carries,0.0) AS carries,
      COALESCE(line_breaks,0.0) AS line_breaks,
      COALESCE(errors,0.0) AS errors,
      involvements
    FROM read_parquet('')
    WHERE player_name ILIKE '%Couchman%'
  ),
  per_game AS (
    SELECT
      player_name,
      minutes,
      80.0*tackles/NULLIF(minutes,0)            AS tackles80,
      80.0*missed_tackles/NULLIF(minutes,0)     AS missed_tackles80,
      80.0*run_metres/NULLIF(minutes,0)         AS runm80,
      80.0*post_contact_metres/NULLIF(minutes,0)AS pcm80,
      80.0*penalties_conceded/NULLIF(minutes,0) AS pens80,
      CASE WHEN carries>0 THEN run_metres/carries ELSE NULL END AS mpc,
      80.0*line_breaks/NULLIF(minutes,0)        AS lb80,
      CASE WHEN (tackles+missed_tackles)>0
           THEN 100.0*tackles/(tackles+missed_tackles) ELSE NULL END AS tack_eff_pct,
      80.0*errors/NULLIF(minutes,0)             AS errors80,
      CASE WHEN involvements IS NOT NULL
           THEN 80.0*involvements/NULLIF(minutes,0) ELSE NULL END AS involvements80
    FROM base
  ),
  agg AS (
    SELECT
      player_name,
      COUNT(*) AS n,
      avg(tackles80) AS tackles80,            stddev_samp(tackles80)           AS sd_tackles80,
      avg(missed_tackles80) AS missed_tackles80, stddev_samp(missed_tackles80) AS sd_missed_tackles80,
      avg(runm80) AS runm80,                  stddev_samp(runm80)              AS sd_runm80,
      avg(pcm80) AS pcm80,                    stddev_samp(pcm80)               AS sd_pcm80,
      avg(pens80) AS pens80,                  stddev_samp(pens80)              AS sd_pens80,
      avg(mpc) AS mpc,                        stddev_samp(mpc)                 AS sd_mpc,
      avg(lb80) AS lb80,                      stddev_samp(lb80)                AS sd_lb80,
      avg(tack_eff_pct) AS tack_eff_pct,      stddev_samp(tack_eff_pct)        AS sd_tack_eff_pct,
      avg(errors80) AS errors80,              stddev_samp(errors80)            AS sd_errors80,
      avg(involvements80) AS involvements80,  stddev_samp(involvements80)      AS sd_involvements80
    FROM per_game
    GROUP BY player_name
  ),
  fmt AS (
    SELECT
      player_name, n,
      tackles80,            1.96*sd_tackles80/sqrt(n)          AS half_tackles80,
      missed_tackles80,     1.96*sd_missed_tackles80/sqrt(n)   AS half_missed_tackles80,
      runm80,               1.96*sd_runm80/sqrt(n)             AS half_runm80,
      pcm80,                1.96*sd_pcm80/sqrt(n)              AS half_pcm80,
      pens80,               1.96*sd_pens80/sqrt(n)             AS half_pens80,
      mpc,                  1.96*sd_mpc/sqrt(n)                AS half_mpc,
      lb80,                 1.96*sd_lb80/sqrt(n)               AS half_lb80,
      tack_eff_pct,         1.96*sd_tack_eff_pct/sqrt(n)       AS half_tack_eff_pct,
      errors80,             1.96*sd_errors80/sqrt(n)           AS half_errors80,
      involvements80,       1.96*sd_involvements80/sqrt(n)     AS half_involvements80
    FROM agg
  )
  SELECT 'Tackles/80'          AS metric,
         ROUND(tackles80,1)    AS value,
         ROUND(tackles80 - half_tackles80,1) AS ci_low,
         ROUND(tackles80 + half_tackles80,1) AS ci_high,
         'Work-rate; adjusted for minutes'   AS note
  FROM fmt WHERE lower(player_name) LIKE lower('%%')
  UNION ALL SELECT 'Missed tackles/80', ROUND(missed_tackles80,1), ROUND(missed_tackles80 - half_missed_tackles80,1), ROUND(missed_tackles80 + half_missed_tackles80,1), 'Expected band by role' FROM fmt WHERE lower(player_name) LIKE lower('%%')
  UNION ALL SELECT 'Run metres/80', ROUND(runm80,1), ROUND(runm80 - half_runm80,1), ROUND(runm80 + half_runm80,1), 'Carries incl. returns' FROM fmt WHERE lower(player_name) LIKE lower('%%')
  UNION ALL SELECT 'Post-contact m', ROUND(pcm80,1), ROUND(pcm80 - half_pcm80,1), ROUND(pcm80 + half_pcm80,1), 'NRL.com definition' FROM fmt WHERE lower(player_name) LIKE lower('%%')
  UNION ALL SELECT 'Penalties conceded/80', ROUND(pens80,2), ROUND(pens80 - half_pens80,2), ROUND(pens80 + half_pens80,2), 'Discipline' FROM fmt WHERE lower(player_name) LIKE lower('%%')
  UNION ALL SELECT 'Metres/carry', ROUND(mpc,2), ROUND(mpc - half_mpc,2), ROUND(mpc + half_mpc,2), 'Carry efficiency' FROM fmt WHERE lower(player_name) LIKE lower('%%')
  UNION ALL SELECT 'Line breaks/80', ROUND(lb80,2), ROUND(lb80 - half_lb80,2), ROUND(lb80 + half_lb80,2), 'Volatile small-sample' FROM fmt WHERE lower(player_name) LIKE lower('%%')
  UNION ALL SELECT 'Tackle eff. %', ROUND(tack_eff_pct,1), ROUND(tack_eff_pct - half_tack_eff_pct,1), ROUND(tack_eff_pct + half_tack_eff_pct,1), 'Role-adjusted' FROM fmt WHERE lower(player_name) LIKE lower('%%')
  UNION ALL SELECT 'Errors/80', ROUND(errors80,2), ROUND(errors80 - half_errors80,2), ROUND(errors80 + half_errors80,2), 'Stability' FROM fmt WHERE lower(player_name) LIKE lower('%%')
  UNION ALL SELECT 'Involvements/80', ROUND(involvements80,1), ROUND(involvements80 - half_involvements80,1), ROUND(involvements80 + half_involvements80,1), 'Usage' FROM fmt WHERE lower(player_name) LIKE lower('%%')
) TO 'data/metrics_toby.csv' WITH (HEADER, DELIMITER ',');

COPY (
  WITH base AS (
    SELECT
      COALESCE(player_name,'') AS player_name,
      COALESCE(minutes_played,0.0) AS minutes,
      COALESCE(tackles,0.0) AS tackles,
      COALESCE(missed_tackles,0.0) AS missed_tackles,
      COALESCE(run_metres,0.0) AS run_metres,
      COALESCE(post_contact_metres,0.0) AS post_contact_metres,
      COALESCE(penalties_conceded,0.0) AS penalties_conceded,
      COALESCE(carries,0.0) AS carries,
      COALESCE(line_breaks,0.0) AS line_breaks,
      COALESCE(errors,0.0) AS errors,
      involvements
    FROM read_parquet('')
    WHERE player_name ILIKE '%Couchman%'
  ),
  per_game AS (
    SELECT
      player_name,
      minutes,
      80.0*tackles/NULLIF(minutes,0)            AS tackles80,
      80.0*missed_tackles/NULLIF(minutes,0)     AS missed_tackles80,
      80.0*run_metres/NULLIF(minutes,0)         AS runm80,
      80.0*post_contact_metres/NULLIF(minutes,0)AS pcm80,
      80.0*penalties_conceded/NULLIF(minutes,0) AS pens80,
      CASE WHEN carries>0 THEN run_metres/carries ELSE NULL END AS mpc,
      80.0*line_breaks/NULLIF(minutes,0)        AS lb80,
      CASE WHEN (tackles+missed_tackles)>0
           THEN 100.0*tackles/(tackles+missed_tackles) ELSE NULL END AS tack_eff_pct,
      80.0*errors/NULLIF(minutes,0)             AS errors80,
      CASE WHEN involvements IS NOT NULL
           THEN 80.0*involvements/NULLIF(minutes,0) ELSE NULL END AS involvements80
    FROM base
  ),
  agg AS (
    SELECT
      player_name,
      COUNT(*) AS n,
      avg(tackles80) AS tackles80,            stddev_samp(tackles80)           AS sd_tackles80,
      avg(missed_tackles80) AS missed_tackles80, stddev_samp(missed_tackles80) AS sd_missed_tackles80,
      avg(runm80) AS runm80,                  stddev_samp(runm80)              AS sd_runm80,
      avg(pcm80) AS pcm80,                    stddev_samp(pcm80)               AS sd_pcm80,
      avg(pens80) AS pens80,                  stddev_samp(pens80)              AS sd_pens80,
      avg(mpc) AS mpc,                        stddev_samp(mpc)                 AS sd_mpc,
      avg(lb80) AS lb80,                      stddev_samp(lb80)                AS sd_lb80,
      avg(tack_eff_pct) AS tack_eff_pct,      stddev_samp(tack_eff_pct)        AS sd_tack_eff_pct,
      avg(errors80) AS errors80,              stddev_samp(errors80)            AS sd_errors80,
      avg(involvements80) AS involvements80,  stddev_samp(involvements80)      AS sd_involvements80
    FROM per_game
    GROUP BY player_name
  ),
  fmt AS (
    SELECT
      player_name, n,
      tackles80,            1.96*sd_tackles80/sqrt(n)          AS half_tackles80,
      missed_tackles80,     1.96*sd_missed_tackles80/sqrt(n)   AS half_missed_tackles80,
      runm80,               1.96*sd_runm80/sqrt(n)             AS half_runm80,
      pcm80,                1.96*sd_pcm80/sqrt(n)              AS half_pcm80,
      pens80,               1.96*sd_pens80/sqrt(n)             AS half_pens80,
      mpc,                  1.96*sd_mpc/sqrt(n)                AS half_mpc,
      lb80,                 1.96*sd_lb80/sqrt(n)               AS half_lb80,
      tack_eff_pct,         1.96*sd_tack_eff_pct/sqrt(n)       AS half_tack_eff_pct,
      errors80,             1.96*sd_errors80/sqrt(n)           AS half_errors80,
      involvements80,       1.96*sd_involvements80/sqrt(n)     AS half_involvements80
    FROM agg
  )
  SELECT 'Metres/carry', ROUND(mpc,2), ROUND(mpc - half_mpc,2), ROUND(mpc + half_mpc,2), 'Carry efficiency' FROM fmt WHERE lower(player_name) LIKE lower('%%')
  UNION ALL SELECT 'Line breaks/80', ROUND(lb80,2), ROUND(lb80 - half_lb80,2), ROUND(lb80 + half_lb80,2), 'Volatile small-sample' FROM fmt WHERE lower(player_name) LIKE lower('%%')
  UNION ALL SELECT 'Tackle eff. %', ROUND(tack_eff_pct,1), ROUND(tack_eff_pct - half_tack_eff_pct,1), ROUND(tack_eff_pct + half_tack_eff_pct,1), 'Role-adjusted' FROM fmt WHERE lower(player_name) LIKE lower('%%')
  UNION ALL SELECT 'Errors/80', ROUND(errors80,2), ROUND(errors80 - half_errors80,2), ROUND(errors80 + half_errors80,2), 'Stability' FROM fmt WHERE lower(player_name) LIKE lower('%%')
  UNION ALL SELECT 'Involvements/80', ROUND(involvements80,1), ROUND(involvements80 - half_involvements80,1), ROUND(involvements80 + half_involvements80,1), 'Usage' FROM fmt WHERE lower(player_name) LIKE lower('%%')
  UNION ALL SELECT 'Run metres/80', ROUND(runm80,1), ROUND(runm80 - half_runm80,1), ROUND(runm80 + half_runm80,1), 'Carries incl. returns' FROM fmt WHERE lower(player_name) LIKE lower('%%')
) TO 'data/metrics_ryan.csv' WITH (HEADER, DELIMITER ',');
