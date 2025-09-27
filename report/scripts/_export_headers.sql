COPY (
  WITH f AS (SELECT * FROM read_parquet('/Users/joelcouchman/FullDataNRL/data/curated/fact_player_game.parquet')),
       d AS (SELECT COALESCE(full_name, (first_name || ' ' || last_name)) AS name_full, player_id
             FROM read_parquet('/Users/joelcouchman/FullDataNRL/data/curated/dim_player.parquet')),
       base AS (
         SELECT
           d.name_full AS player_name,
           COALESCE(NULL, 0.0) AS minutes,
           COALESCE(NULL, 0.0) AS tackles,
           COALESCE(NULL, 0.0) AS missed_tackles,
           COALESCE(NULL, 0.0) AS run_metres,
           COALESCE(NULL, 0.0) AS post_contact_metres,
           COALESCE(NULL, 0.0) AS penalties_conceded,
           COALESCE(NULL, 0.0) AS carries,
           COALESCE(NULL, 0.0) AS line_breaks,
           COALESCE(NULL, 0.0) AS errors
         FROM f JOIN d USING (player_id)
         WHERE d.name_full ILIKE '%Couchman%'
       ),
       per_game AS (
         SELECT
           player_name,
           minutes,
           80.0*tackles/NULLIF(minutes,0)             AS tackles80,
           80.0*missed_tackles/NULLIF(minutes,0)      AS missed_tackles80,
           80.0*run_metres/NULLIF(minutes,0)          AS runm80,
           80.0*post_contact_metres/NULLIF(minutes,0) AS pcm80,
           80.0*penalties_conceded/NULLIF(minutes,0)  AS pens80,
           CASE WHEN carries>0 THEN run_metres/carries ELSE NULL END AS mpc,
           80.0*line_breaks/NULLIF(minutes,0)         AS lb80,
           CASE WHEN (tackles+missed_tackles)>0
                THEN 100.0*tackles/(tackles+missed_tackles) ELSE NULL END AS tack_eff_pct,
           80.0*errors/NULLIF(minutes,0)              AS errors80
         FROM base
       ),
       agg AS (
         SELECT
           player_name,
           COUNT(*) AS n,
           AVG(tackles80) AS tackles80,                      STDDEV_SAMP(tackles80)         AS sd_tackles80,
           AVG(missed_tackles80) AS missed_tackles80,        STDDEV_SAMP(missed_tackles80)  AS sd_missed_tackles80,
           AVG(runm80) AS runm80,                            STDDEV_SAMP(runm80)            AS sd_runm80,
           AVG(pcm80) AS pcm80,                              STDDEV_SAMP(pcm80)             AS sd_pcm80,
           AVG(pens80) AS pens80,                            STDDEV_SAMP(pens80)            AS sd_pens80,
           AVG(mpc) AS mpc,                                  STDDEV_SAMP(mpc)               AS sd_mpc,
           AVG(lb80) AS lb80,                                STDDEV_SAMP(lb80)              AS sd_lb80,
           AVG(tack_eff_pct) AS tack_eff_pct,                STDDEV_SAMP(tack_eff_pct)      AS sd_tack_eff_pct,
           AVG(errors80) AS errors80,                        STDDEV_SAMP(errors80)          AS sd_errors80
         FROM per_game
         GROUP BY player_name
       )
  SELECT
    'Tackles/80'                                           AS metric,
    ROUND(tackles80,1)                                     AS value,
    ROUND(tackles80 - 1.96*sd_tackles80/NULLIF(SQRT(n),0),1) AS ci_low,
    ROUND(tackles80 + 1.96*sd_tackles80/NULLIF(SQRT(n),0),1) AS ci_high,
    'Work-rate; adjusted for minutes'                      AS note
    FROM agg WHERE lower(player_name) LIKE lower('%Toby Couchman%')
  UNION ALL SELECT 'Missed tackles/80',
    ROUND(missed_tackles80,1),
    ROUND(missed_tackles80 - 1.96*sd_missed_tackles80/NULLIF(SQRT(n),0),1),
    ROUND(missed_tackles80 + 1.96*sd_missed_tackles80/NULLIF(SQRT(n),0),1),
    'Expected band by role'
    FROM agg WHERE lower(player_name) LIKE lower('%Toby Couchman%')
  UNION ALL SELECT 'Run metres/80',
    ROUND(runm80,1),
    ROUND(runm80 - 1.96*sd_runm80/NULLIF(SQRT(n),0),1),
    ROUND(runm80 + 1.96*sd_runm80/NULLIF(SQRT(n),0),1),
    'Carries incl. returns'
    FROM agg WHERE lower(player_name) LIKE lower('%Toby Couchman%')
  UNION ALL SELECT 'Post-contact m',
    ROUND(pcm80,1),
    ROUND(pcm80 - 1.96*sd_pcm80/NULLIF(SQRT(n),0),1),
    ROUND(pcm80 + 1.96*sd_pcm80/NULLIF(SQRT(n),0),1),
    'NRL.com definition'
    FROM agg WHERE lower(player_name) LIKE lower('%Toby Couchman%')
  UNION ALL SELECT 'Penalties conceded/80',
    ROUND(pens80,2),
    ROUND(pens80 - 1.96*sd_pens80/NULLIF(SQRT(n),0),2),
    ROUND(pens80 + 1.96*sd_pens80/NULLIF(SQRT(n),0),2),
    'Discipline'
    FROM agg WHERE lower(player_name) LIKE lower('%Toby Couchman%')
  UNION ALL SELECT 'Metres/carry',
    ROUND(mpc,2),
    ROUND(mpc - 1.96*sd_mpc/NULLIF(SQRT(n),0),2),
    ROUND(mpc + 1.96*sd_mpc/NULLIF(SQRT(n),0),2),
    'Carry efficiency'
    FROM agg WHERE lower(player_name) LIKE lower('%Toby Couchman%')
  UNION ALL SELECT 'Line breaks/80',
    ROUND(lb80,2),
    ROUND(lb80 - 1.96*sd_lb80/NULLIF(SQRT(n),0),2),
    ROUND(lb80 + 1.96*sd_lb80/NULLIF(SQRT(n),0),2),
    'Volatile small-sample'
    FROM agg WHERE lower(player_name) LIKE lower('%Toby Couchman%')
  UNION ALL SELECT 'Tackle eff. %',
    ROUND(tack_eff_pct,1),
    ROUND(tack_eff_pct - 1.96*sd_tack_eff_pct/NULLIF(SQRT(n),0),1),
    ROUND(tack_eff_pct + 1.96*sd_tack_eff_pct/NULLIF(SQRT(n),0),1),
    'Role-adjusted'
    FROM agg WHERE lower(player_name) LIKE lower('%Toby Couchman%')
  UNION ALL SELECT 'Errors/80',
    ROUND(errors80,2),
    ROUND(errors80 - 1.96*sd_errors80/NULLIF(SQRT(n),0),2),
    ROUND(errors80 + 1.96*sd_errors80/NULLIF(SQRT(n),0),2),
    'Stability'
    FROM agg WHERE lower(player_name) LIKE lower('%Toby Couchman%')
) TO 'data/metrics_toby.csv' WITH (HEADER, DELIMITER ',');

COPY (
  WITH f AS (SELECT * FROM read_parquet('/Users/joelcouchman/FullDataNRL/data/curated/fact_player_game.parquet')),
       d AS (SELECT COALESCE(full_name, (first_name || ' ' || last_name)) AS name_full, player_id
             FROM read_parquet('/Users/joelcouchman/FullDataNRL/data/curated/dim_player.parquet')),
       base AS (
         SELECT
           d.name_full AS player_name,
           COALESCE(NULL, 0.0) AS minutes,
           COALESCE(NULL, 0.0) AS tackles,
           COALESCE(NULL, 0.0) AS missed_tackles,
           COALESCE(NULL, 0.0) AS run_metres,
           COALESCE(NULL, 0.0) AS post_contact_metres,
           COALESCE(NULL, 0.0) AS penalties_conceded,
           COALESCE(NULL, 0.0) AS carries,
           COALESCE(NULL, 0.0) AS line_breaks,
           COALESCE(NULL, 0.0) AS errors
         FROM f JOIN d USING (player_id)
         WHERE d.name_full ILIKE '%Couchman%'
       ),
       per_game AS (
         SELECT
           player_name,
           minutes,
           80.0*tackles/NULLIF(minutes,0)             AS tackles80,
           80.0*missed_tackles/NULLIF(minutes,0)      AS missed_tackles80,
           80.0*run_metres/NULLIF(minutes,0)          AS runm80,
           80.0*post_contact_metres/NULLIF(minutes,0) AS pcm80,
           80.0*penalties_conceded/NULLIF(minutes,0)  AS pens80,
           CASE WHEN carries>0 THEN run_metres/carries ELSE NULL END AS mpc,
           80.0*line_breaks/NULLIF(minutes,0)         AS lb80,
           CASE WHEN (tackles+missed_tackles)>0
                THEN 100.0*tackles/(tackles+missed_tackles) ELSE NULL END AS tack_eff_pct,
           80.0*errors/NULLIF(minutes,0)              AS errors80
         FROM base
       ),
       agg AS (
         SELECT
           player_name,
           COUNT(*) AS n,
           AVG(mpc) AS mpc,                 STDDEV_SAMP(mpc)          AS sd_mpc,
           AVG(lb80) AS lb80,               STDDEV_SAMP(lb80)         AS sd_lb80,
           AVG(tack_eff_pct) AS tack_eff_pct, STDDEV_SAMP(tack_eff_pct) AS sd_tack_eff_pct,
           AVG(errors80) AS errors80,       STDDEV_SAMP(errors80)     AS sd_errors80,
           AVG(runm80) AS runm80,           STDDEV_SAMP(runm80)       AS sd_runm80
         FROM per_game
         GROUP BY player_name
       )
  SELECT
    'Metres/carry'                               AS metric,
    ROUND(mpc,2)                                 AS value,
    ROUND(mpc - 1.96*sd_mpc/NULLIF(SQRT(n),0),2) AS ci_low,
    ROUND(mpc + 1.96*sd_mpc/NULLIF(SQRT(n),0),2) AS ci_high,
    'Carry efficiency'                           AS note
    FROM agg WHERE lower(player_name) LIKE lower('%Ryan Couchman%')
  UNION ALL SELECT 'Line breaks/80',
    ROUND(lb80,2),
    ROUND(lb80 - 1.96*sd_lb80/NULLIF(SQRT(n),0),2),
    ROUND(lb80 + 1.96*sd_lb80/NULLIF(SQRT(n),0),2),
    'Volatile small-sample'
    FROM agg WHERE lower(player_name) LIKE lower('%Ryan Couchman%')
  UNION ALL SELECT 'Tackle eff. %',
    ROUND(tack_eff_pct,1),
    ROUND(tack_eff_pct - 1.96*sd_tack_eff_pct/NULLIF(SQRT(n),0),1),
    ROUND(tack_eff_pct + 1.96*sd_tack_eff_pct/NULLIF(SQRT(n),0),1),
    'Role-adjusted'
    FROM agg WHERE lower(player_name) LIKE lower('%Ryan Couchman%')
  UNION ALL SELECT 'Errors/80',
    ROUND(errors80,2),
    ROUND(errors80 - 1.96*sd_errors80/NULLIF(SQRT(n),0),2),
    ROUND(errors80 + 1.96*sd_errors80/NULLIF(SQRT(n),0),2),
    'Stability'
    FROM agg WHERE lower(player_name) LIKE lower('%Ryan Couchman%')
  UNION ALL SELECT 'Run metres/80',
    ROUND(runm80,1),
    ROUND(runm80 - 1.96*sd_runm80/NULLIF(SQRT(n),0),1),
    ROUND(runm80 + 1.96*sd_runm80/NULLIF(SQRT(n),0),1),
    'Carries incl. returns'
    FROM agg WHERE lower(player_name) LIKE lower('%Ryan Couchman%')
) TO 'data/metrics_ryan.csv' WITH (HEADER, DELIMITER ',');
