# Crickedge - IPL Cricket Analytics SaaS

## Overview
Cricket analytics SaaS platform for IPL analysis. Covers data engineering, statistical modeling, win probability prediction, and betting edge analysis using historical match data, Elo ratings, and bookmaker odds.

## Project Structure
```
app.py                 # Streamlit dashboard (6 pages)
.streamlit/config.toml # Streamlit config (port 5000)
data/
  raw_json/            # 1169 Cricsheet IPL JSON match files
  processed/           # Generated CSV outputs
    match_metadata.csv
    powerplay_summary.csv
    over_state_snapshots.csv
    elo_ratings_history.csv
    current_team_ratings.csv
    over_state_snapshots_enriched.csv
    statistical_bucket_model_stabilized.csv
    rolling_backtest_results.csv
    historical_odds_raw.csv
    historical_odds_normalized.csv
    match_metadata_with_odds.csv
    edge_simulation_results.csv
    live_model_calibration_2020_plus.csv
    high_confidence_snapshots_85_plus.csv
    high_confidence_inplay_odds.csv
    live_edge_simulation_results.csv
    match_slot_cache.json
scripts/
  build_metadata.py
  build_powerplay_summary.py
  build_over_state_snapshots.py
  build_elo_ratings.py
  build_enriched_datasets.py
  build_statistical_bucket_model.py (aka build_stabilized_model.py)
  build_rolling_backtest.py
  fetch_historical_odds.py
  normalize_odds.py
  merge_odds_metadata.py
  edge_simulation.py
  live_model_calibration.py
  extract_high_confidence_snapshots.py
  fetch_inplay_pinnacle_pilot.py
  simulate_live_edge_85_plus.py
```

## Dashboard (app.py)
Streamlit app with 6 pages navigated via sidebar radio buttons:
1. **Overview** — KPIs, team ratings bar chart, pre-match & in-play edge summaries
2. **Elo Ratings** — Rating trajectories (multi-team line chart), current standings table
3. **Model Calibration** — Calibration plot (predicted vs actual), rolling backtest results
4. **Pre-Match Edge** — Edge distribution histogram, simulation results, match details with slider filter
5. **In-Play Edge** — In-play edge distribution, phase analysis, simulation results, trade log
6. **Bucket Model** — Interactive model explorer (4 selectors), heatmap, sample distribution

## Scripts

### Phase 1: Data Engineering
- **build_metadata.py** — Extracts match-level metadata from JSON files. Skips DLS/no-winner. Output: match_metadata.csv (1124 matches)
- **build_powerplay_summary.py** — Overs 1-6 powerplay stats. Output: powerplay_summary.csv
- **build_over_state_snapshots.py** — Cumulative state at end of every over for both innings. Output: over_state_snapshots.csv (43,593 rows)

### Phase 2: Elo Ratings
- **build_elo_ratings.py** — K=20, initial=1500, standard Elo. Pre-match ratings only (no leakage). Franchise renames normalized. Output: elo_ratings_history.csv, current_team_ratings.csv

### Phase 3-4: Enriched Datasets & Statistical Model
- **build_enriched_datasets.py** — Merges Elo ratings with over-state snapshots and powerplay data. Zero null values.
- **build_statistical_bucket_model.py** — Hierarchical stabilization (Level 1-5), min 50 samples per bucket. 465 buckets, 216 stable. Output: statistical_bucket_model_stabilized.csv

### Phase 5: Rolling Backtest
- **build_rolling_backtest.py** — 3-year train, 1-year test, 15 windows. Avg Brier: 0.218, Avg accuracy: 63.4%. Output: rolling_backtest_results.csv

### Phase 7: Odds & Edge Simulation
- **fetch_historical_odds.py** — Fetches Pinnacle h2h closing odds from The Odds API v4 historical endpoint. Region: eu. Pre-match snapshot at 13:30 UTC. Supports resume via dates cache. Output: historical_odds_raw.csv (341 matches, 2020-2025)
- **normalize_odds.py** — Converts decimal odds to implied probabilities, removes overround. Avg overround: 1.0354. Output: historical_odds_normalized.csv
- **merge_odds_metadata.py** — Merges normalized odds into match_metadata. Output: match_metadata_with_odds.csv
- **edge_simulation.py** — Flat 1-unit betting, tests thresholds 3/5/7/10%. Uses Elo expected win probabilities vs Pinnacle market probabilities. Output: edge_simulation_results.csv

### Phase 8: Live Model Calibration
- **live_model_calibration.py** — Brier score 0.2012, excellent calibration across all deciles (max error 4.3%)

### Phase 9: In-Play Analysis
- **extract_high_confidence_snapshots.py** — 1,404 snapshots at 85%+ confidence from 225 matches
- **fetch_inplay_pinnacle_pilot.py** — In-play Pinnacle odds fetching. Dual slot detection (10:00 and 14:00 UTC for IPL afternoon/evening matches). Slot cache per match. 200 snapshots with odds from 31 2025 matches. Output: high_confidence_inplay_odds.csv
- **simulate_live_edge_85_plus.py** — In-play edge simulation: 84% win rate, 67.2% ROI across 31 trades, max drawdown 1 unit. Output: live_edge_simulation_results.csv

## Data Notes
- Total raw files: 1169, Valid matches: 1124 (no DLS, has winner)
- 15 unique IPL teams with franchise rename normalization
- Elo uses pre-match ratings only — no future leakage
- Odds API: sport key "cricket_ipl", Pinnacle bookmaker, eu region, h2h market, decimal format
- Historical odds available from June 2020 onward
- Pre-match snapshot time: T13:30:00Z (30 min before typical IPL start)
- IPL time slots: afternoon 10:00 UTC (3:30 PM IST), evening 14:00 UTC (7:30 PM IST)
- Team normalization: RCB Bangalore→Bengaluru, Delhi Daredevils→Capitals, Kings XI→Punjab Kings, RPSupergiants→Supergiant

## Environment
- ODDS_API_KEY: Required for fetch scripts (The Odds API v4, paid plan needed for historical data)
- SESSION_SECRET: App session secret

## Recent Changes
- 2026-02-16: Phase 9 fix — Re-fetched in-play odds with corrected timestamps and multi-bookmaker support (pinnacle, betfair_ex_eu, sport888, etc). 1348 snapshots from 217 matches. Real in-play edge: 50-61% ROI, 80% win rate across thresholds. Optimized fetch to 1 API call per match-innings group.
- 2026-02-16: Phase 9 — In-play Pinnacle odds fetching (dual slot detection), live edge simulation (67% ROI), Streamlit dashboard built with 6 pages
- 2026-02-16: Phase 8 — Live model calibration (Brier 0.2012, excellent calibration)
- 2026-02-16: Phase 7 — Odds API integration, normalization, merge, edge simulation complete
- 2026-02-16: Phase 5 — Rolling backtest (15 windows, avg Brier 0.218)
- 2026-02-16: Phase 3-4 — Enriched datasets and statistical bucket model
- 2026-02-16: Phase 2 — Elo rating system built
- 2026-02-16: Phase 1 — Data engineering foundation built
