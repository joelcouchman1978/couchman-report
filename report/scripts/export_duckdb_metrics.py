import os, duckdb, pandas as pd, pathlib, math

ROOT = pathlib.Path(__file__).resolve().parents[1]
out_dir = ROOT / "data"
out_dir.mkdir(parents=True, exist_ok=True)

PLAYER_A = os.environ.get("PLAYER_A_NAME", "Toby Couchman")
PLAYER_B = os.environ.get("PLAYER_B_NAME", "Ryan Couchman")

candidates = [
    os.path.expanduser(os.environ.get("STATS_PARQUET","~/FullDataNRL/data/curated/fact_player_match.parquet")),
    os.path.expanduser("~/FullDataNRL/data/curated/player_match.parquet"),
    os.path.expanduser("~/FullDataNRL/data/curated/player_game_stats.parquet"),
]

src = None
for p in candidates:
    if os.path.exists(p):
        src = p
        break
if src is None:
    raise SystemExit("Set STATS_PARQUET to a player-match parquet file")

con = duckdb.connect()
con.execute("PRAGMA threads=4")

sql = f"""
WITH base AS (
  SELECT
    COALESCE(player_name, '') AS player_name,
    COALESCE(minutes_played, 0.0) AS minutes,
    COALESCE(tackles, 0.0) AS tackles,
    COALESCE(missed_tackles, 0.0) AS missed_tackles,
    COALESCE(run_metres, 0.0) AS run_metres,
    COALESCE(post_contact_metres, 0.0) AS post_contact_metres,
    COALESCE(penalties_conceded, 0.0) AS penalties_conceded,
    COALESCE(carries, 0.0) AS carries,
    COALESCE(line_breaks, 0.0) AS line_breaks,
    COALESCE(errors, 0.0) AS errors,
    COALESCE(involvements, NULL) AS involvements
  FROM read_parquet('{src}')
  WHERE player_name ILIKE '%Couchman%'
),
per_game AS (
  SELECT
    player_name,
    NULLIF(minutes,0) AS minutes_nonzero,
    80.0 * tackles/NULLIF(minutes,0) AS tackles80,
    80.0 * missed_tackles/NULLIF(minutes,0) AS missed_tackles80,
    80.0 * run_metres/NULLIF(minutes,0) AS runm80,
    80.0 * post_contact_metres/NULLIF(minutes,0) AS pcm80,
    80.0 * penalties_conceded/NULLIF(minutes,0) AS pens80,
    CASE WHEN carries>0 THEN run_metres/carries ELSE NULL END AS metres_per_carry,
    80.0 * line_breaks/NULLIF(minutes,0) AS lb80,
    CASE WHEN (tackles+missed_tackles)>0 THEN 100.0*tackles/(tackles+missed_tackles) ELSE NULL END AS tack_eff_pct,
    80.0 * errors/NULLIF(minutes,0) AS errors80,
    CASE WHEN involvements IS NOT NULL THEN 80.0*involvements/NULLIF(minutes,0) ELSE NULL END AS involvements80
  FROM base
),
agg AS (
  SELECT
    player_name,
    COUNT(*) AS n,
    AVG(tackles80) AS tackles80,
    STDDEV_SAMP(tackles80) AS sd_tackles80,
    AVG(missed_tackles80) AS missed_tackles80,
    STDDEV_SAMP(missed_tackles80) AS sd_missed_tackles80,
    AVG(runm80) AS runm80,
    STDDEV_SAMP(runm80) AS sd_runm80,
    AVG(pcm80) AS pcm80,
    STDDEV_SAMP(pcm80) AS sd_pcm80,
    AVG(pens80) AS pens80,
    STDDEV_SAMP(pens80) AS sd_pens80,
    AVG(metres_per_carry) AS mpc,
    STDDEV_SAMP(metres_per_carry) AS sd_mpc,
    AVG(lb80) AS lb80,
    STDDEV_SAMP(lb80) AS sd_lb80,
    AVG(tack_eff_pct) AS tack_eff_pct,
    STDDEV_SAMP(tack_eff_pct) AS sd_tack_eff_pct,
    AVG(errors80) AS errors80,
    STDDEV_SAMP(errors80) AS sd_errors80,
    AVG(involvements80) AS involvements80,
    STDDEV_SAMP(involvements80) AS sd_involvements80
  FROM per_game
  GROUP BY player_name
)
SELECT * FROM agg
WHERE player_name ILIKE '%Couchman%'
"""
df = con.execute(sql).fetchdf()

def ci(mean, sd, n):
    if n is None or n < 2 or sd is None or math.isnan(sd) or mean is None or math.isnan(mean):
        return (None, None)
    half = 1.96 * sd / math.sqrt(n)
    return (mean - half, mean + half)

def rows_for(player):
    r = df[df["player_name"].str.lower().str.contains(player.lower())]
    if r.empty:
        return []
    r = r.iloc[0]
    n = int(r["n"])
    out = []
    m, sd = r["tackles80"], r["sd_tackles80"]; lo, hi = ci(m, sd, n); out.append(("Tackles/80", m, lo, hi, "Work-rate; adjusted for minutes"))
    m, sd = r["missed_tackles80"], r["sd_missed_tackles80"]; lo, hi = ci(m, sd, n); out.append(("Missed tackles/80", m, lo, hi, "Expected band by role"))
    m, sd = r["runm80"], r["sd_runm80"]; lo, hi = ci(m, sd, n); out.append(("Run metres/80", m, lo, hi, "Carries incl. returns"))
    m, sd = r["pcm80"], r["sd_pcm80"]; lo, hi = ci(m, sd, n); out.append(("Post-contact m", m, lo, hi, "NRL.com definition"))
    m, sd = r["pens80"], r["sd_pens80"]; lo, hi = ci(m, sd, n); out.append(("Penalties conceded/80", m, lo, hi, "Discipline"))
    m, sd = r["mpc"], r["sd_mpc"]; lo, hi = ci(m, sd, n); out.append(("Metres/carry", m, lo, hi, "Carry efficiency"))
    m, sd = r["lb80"], r["sd_lb80"]; lo, hi = ci(m, sd, n); out.append(("Line breaks/80", m, lo, hi, "Volatile small-sample"))
    m, sd = r["tack_eff_pct"], r["sd_tack_eff_pct"]; lo, hi = ci(m, sd, n); out.append(("Tackle eff. %", m, lo, hi, "Role-adjusted"))
    m, sd = r["errors80"], r["sd_errors80"]; lo, hi = ci(m, sd, n); out.append(("Errors/80", m, lo, hi, "Stability"))
    m, sd = r["involvements80"], r["sd_involvements80"]; lo, hi = ci(m, sd, n); out.append(("Involvements/80", m, lo, hi, "Usage"))
    clean = []
    for (metric, v, lo, hi, note) in out:
        def fmt(x):
            if x is None or pd.isna(x):
                return ""
            return f"{x:.3g}"
        clean.append({"metric": metric, "value": fmt(v), "ci_low": fmt(lo), "ci_high": fmt(hi), "note": note})
    return clean

toby_rows = rows_for(PLAYER_A)
ryan_rows = rows_for(PLAYER_B)

pd.DataFrame(toby_rows, columns=["metric","value","ci_low","ci_high","note"]).to_csv(out_dir/"metrics_toby.csv", index=False)
pd.DataFrame(ryan_rows, columns=["metric","value","ci_low","ci_high","note"]).to_csv(out_dir/"metrics_ryan.csv", index=False)
print("Wrote CSVs:", out_dir/"metrics_toby.csv", out_dir/"metrics_ryan.csv")
