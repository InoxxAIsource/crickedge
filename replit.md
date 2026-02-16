# Crickedge - IPL Cricket Analytics SaaS

## Overview
Phase 1: IPL Historical Data Engineering Foundation. Processes Cricsheet-format IPL JSON data into clean CSV datasets for analytics.

## Project Structure
```
data/
  raw_json/          # 1169 Cricsheet IPL JSON match files
  processed/         # Generated CSV outputs
    match_metadata.csv
    powerplay_summary.csv
    over_state_snapshots.csv
scripts/
  build_metadata.py
  build_powerplay_summary.py
  build_over_state_snapshots.py
  build_elo_ratings.py
```

## Scripts

### build_metadata.py
- Extracts match-level metadata from all JSON files
- Skips matches with no winner or DLS method
- Output: data/processed/match_metadata.csv (1124 valid matches)

### build_powerplay_summary.py
- Extracts first innings overs 1-6 powerplay stats
- Calculates powerplay_runs and powerplay_wickets
- Output: data/processed/powerplay_summary.csv

### build_over_state_snapshots.py
- Creates cumulative state at end of every over for both innings
- Second innings includes target, balls_remaining, required_run_rate
- Output: data/processed/over_state_snapshots.csv (43,593 rows)

## Data Notes
- Total raw files: 1169
- Valid matches (no DLS, has winner): 1124
- Skipped no-winner: 23, Skipped DLS: 22
- Season field can be str or int in source JSON; normalized to str in outputs
- Cricsheet JSON format: keys are meta, info, innings
- Overs are 0-indexed in source (over 0 = first over); outputs use 1-indexed over_number

### build_elo_ratings.py
- Reads match_metadata.csv, sorts by date, computes Elo ratings chronologically
- K-factor=20, initial rating=1500, standard Elo formula
- No future leakage: pre-match ratings recorded before update
- Output: data/processed/elo_ratings_history.csv (per-match ratings history)
- Output: data/processed/current_team_ratings.csv (final standings)
- Note: franchise renames (e.g. Royal Challengers Bangalore → Bengaluru) appear as separate teams in source data

## Recent Changes
- 2026-02-16: Phase 2 — Elo rating system built
- 2026-02-16: Initial data engineering foundation built (Phase 1)
