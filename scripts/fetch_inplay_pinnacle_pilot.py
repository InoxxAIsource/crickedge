import csv
import json
import os
import sys
import time
import requests
from collections import defaultdict


PROCESSED_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "processed")
INPUT_PATH = os.path.join(PROCESSED_DIR, "high_confidence_snapshots_85_plus.csv")
METADATA_PATH = os.path.join(PROCESSED_DIR, "match_metadata.csv")
OUTPUT_PATH = os.path.join(PROCESSED_DIR, "high_confidence_inplay_odds.csv")

API_KEY = os.environ.get("ODDS_API_KEY", "")
BASE_URL = "https://api.the-odds-api.com/v4/historical/sports/cricket_ipl/odds"
BOOKMAKER = "pinnacle"
REGION = "eu"
MARKET = "h2h"
ODDS_FORMAT = "decimal"
SLEEP_BETWEEN_CALLS = 0.5
MAX_API_CALLS = 200

OUTPUT_FIELDS = [
    "match_id", "date", "innings_number", "over_number",
    "batting_team", "bowling_team",
    "model_probability", "eventual_winner",
    "market_team_1", "market_team_2",
    "market_odds_1", "market_odds_2",
    "market_prob_1", "market_prob_2",
    "edge", "fetch_timestamp"
]

TEAM_NAME_MAP = {
    "Royal Challengers Bangalore": "Royal Challengers Bengaluru",
    "Delhi Daredevils": "Delhi Capitals",
    "Kings XI Punjab": "Punjab Kings",
    "Rising Pune Supergiants": "Rising Pune Supergiant",
}

MINUTES_PER_OVER = 4
INNINGS_BREAK_MINUTES = 20


def normalize_team(name):
    return TEAM_NAME_MAP.get(name, name)


def determine_match_slot(match_id, date_str, metadata_by_date):
    day_matches = metadata_by_date.get(date_str, [])
    if len(day_matches) <= 1:
        return 14
    sorted_matches = sorted(day_matches, key=lambda m: int(m["match_id"]))
    if str(match_id) == str(sorted_matches[0]["match_id"]):
        return 10
    return 14


def estimate_timestamp(date_str, innings_number, over_number, start_hour):
    innings = int(innings_number)
    over = int(over_number)

    if innings == 1:
        minutes_offset = over * MINUTES_PER_OVER
    else:
        minutes_offset = 20 * MINUTES_PER_OVER + INNINGS_BREAK_MINUTES + over * MINUTES_PER_OVER

    total_minutes = start_hour * 60 + minutes_offset
    hours = total_minutes // 60
    mins = total_minutes % 60
    return f"{date_str}T{hours:02d}:{mins:02d}:00Z"


def load_existing_results():
    if not os.path.exists(OUTPUT_PATH):
        return [], set()
    with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    done = {(r["match_id"], r["innings_number"], r["over_number"]) for r in rows}
    return rows, done


def save_rows(rows):
    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def fetch_odds_snapshot(timestamp):
    params = {
        "apiKey": API_KEY,
        "regions": REGION,
        "markets": MARKET,
        "oddsFormat": ODDS_FORMAT,
        "date": timestamp,
    }
    resp = requests.get(BASE_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def find_pinnacle_for_teams(events, batting_team, bowling_team):
    for event in events:
        event_home = event.get("home_team", "")
        event_away = event.get("away_team", "")

        home_norm = normalize_team(event_home)
        away_norm = normalize_team(event_away)

        teams = {home_norm, away_norm}
        if batting_team not in teams or bowling_team not in teams:
            continue

        for bm in event.get("bookmakers", []):
            if bm["key"] != BOOKMAKER:
                continue
            for market in bm.get("markets", []):
                if market["key"] != MARKET:
                    continue
                outcomes = {}
                for o in market["outcomes"]:
                    outcomes[normalize_team(o["name"])] = o["price"]

                if batting_team in outcomes and bowling_team in outcomes:
                    return outcomes
    return None


def fetch_inplay_pilot():
    if not API_KEY:
        print("ERROR: ODDS_API_KEY environment variable not set.", file=sys.stderr)
        sys.exit(1)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    with open(INPUT_PATH, "r", encoding="utf-8") as f:
        snapshots = list(csv.DictReader(f))

    with open(METADATA_PATH, "r", encoding="utf-8") as f:
        metadata = list(csv.DictReader(f))

    metadata_by_date = defaultdict(list)
    for m in metadata:
        metadata_by_date[m["date"]].append(m)

    existing_rows, done_keys = load_existing_results()

    remaining = [s for s in snapshots
                 if (s["match_id"], s["innings_number"], s["over_number"]) not in done_keys]
    remaining.sort(key=lambda s: s["date"], reverse=True)

    match_snaps = {}
    for s in remaining:
        mid = s["match_id"]
        match_snaps.setdefault(mid, []).append(s)

    print(f"\n=== fetch_inplay_pinnacle_pilot.py ===")
    print(f"  Total high-confidence snapshots : {len(snapshots)}")
    print(f"  Already fetched                 : {len(done_keys)}")
    print(f"  Remaining snapshots             : {len(remaining)}")
    print(f"  Unique matches to process       : {len(match_snaps)}")
    print(f"  Slot detection: schedule-based (single=14:00, double=10:00/14:00)")

    rows = list(existing_rows)
    api_calls = 0
    found = 0
    not_found = 0

    sorted_matches = sorted(match_snaps.keys(),
                            key=lambda mid: match_snaps[mid][0]["date"],
                            reverse=True)

    for mid in sorted_matches:
        if api_calls >= MAX_API_CALLS:
            print(f"  Reached API call limit ({MAX_API_CALLS})")
            break

        snaps_for_match = match_snaps[mid]
        sample = snaps_for_match[0]
        date_str = sample["date"]

        start_hour = determine_match_slot(mid, date_str, metadata_by_date)
        print(f"  Match {mid} ({date_str}): slot {start_hour}:00 UTC")

        ts_groups = {}
        for snap in snaps_for_match:
            ts = estimate_timestamp(date_str, snap["innings_number"], snap["over_number"], start_hour)
            ts_groups.setdefault(ts, []).append(snap)

        for ts in sorted(ts_groups.keys()):
            if api_calls >= MAX_API_CALLS:
                break

            try:
                data = fetch_odds_snapshot(ts)
                events = data.get("data", [])
                api_calls += 1
                time.sleep(SLEEP_BETWEEN_CALLS)
            except Exception as e:
                print(f"  Error {ts}: {e}", file=sys.stderr)
                api_calls += 1
                time.sleep(1.0)
                continue

            for snap in ts_groups[ts]:
                bat = snap["batting_team"]
                bowl = snap["bowling_team"]
                model_prob = float(snap["final_stabilized_probability"])

                odds = find_pinnacle_for_teams(events, bat, bowl)

                if odds and bat in odds and bowl in odds:
                    bat_odds = odds[bat]
                    bowl_odds = odds[bowl]
                    bat_imp = 1.0 / bat_odds
                    bowl_imp = 1.0 / bowl_odds
                    total_imp = bat_imp + bowl_imp
                    bat_market_prob = bat_imp / total_imp
                    bowl_market_prob = bowl_imp / total_imp
                    edge = model_prob - bat_market_prob

                    rows.append({
                        "match_id": snap["match_id"],
                        "date": snap["date"],
                        "innings_number": snap["innings_number"],
                        "over_number": snap["over_number"],
                        "batting_team": bat,
                        "bowling_team": bowl,
                        "model_probability": round(model_prob, 6),
                        "eventual_winner": snap["eventual_winner"],
                        "market_team_1": bat,
                        "market_team_2": bowl,
                        "market_odds_1": bat_odds,
                        "market_odds_2": bowl_odds,
                        "market_prob_1": round(bat_market_prob, 6),
                        "market_prob_2": round(bowl_market_prob, 6),
                        "edge": round(edge, 6),
                        "fetch_timestamp": ts,
                    })
                    found += 1
                else:
                    not_found += 1

        if api_calls % 20 == 0 and api_calls > 0:
            save_rows(rows)
            print(f"  Progress: {api_calls} API calls, {found} found, {not_found} not found")

    save_rows(rows)

    print(f"\n  Results:")
    print(f"    API calls              : {api_calls}")
    print(f"    Snapshots with odds    : {found}")
    print(f"    Snapshots without odds : {not_found}")
    print(f"    Total rows in output   : {len(rows)}")
    print(f"    Output                 : {OUTPUT_PATH}")


if __name__ == "__main__":
    fetch_inplay_pilot()
