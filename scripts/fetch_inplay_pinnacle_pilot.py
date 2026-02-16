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
REGION = "eu"
MARKET = "h2h"
ODDS_FORMAT = "decimal"
SLEEP_BETWEEN_CALLS = 0.5
MAX_API_CALLS = 500

BOOKMAKER_PRIORITY = ["pinnacle", "betfair_ex_eu", "sport888", "williamhill",
                       "marathonbet", "nordicbet", "matchbook", "betonlineag"]

OUTPUT_FIELDS = [
    "match_id", "date", "innings_number", "over_number",
    "batting_team", "bowling_team",
    "model_probability", "eventual_winner",
    "market_team_1", "market_team_2",
    "market_odds_1", "market_odds_2",
    "market_prob_1", "market_prob_2",
    "edge", "fetch_timestamp", "bookmaker_used"
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


def find_best_bookmaker_for_teams(events, batting_team, bowling_team):
    for event in events:
        home_norm = normalize_team(event.get("home_team", ""))
        away_norm = normalize_team(event.get("away_team", ""))

        teams = {home_norm, away_norm}
        if batting_team not in teams or bowling_team not in teams:
            continue

        bookmaker_odds = {}
        for bm in event.get("bookmakers", []):
            for market in bm.get("markets", []):
                if market["key"] != MARKET:
                    continue
                outcomes = {}
                for o in market["outcomes"]:
                    norm_name = normalize_team(o["name"])
                    if norm_name in (batting_team, bowling_team):
                        outcomes[norm_name] = o["price"]
                if batting_team in outcomes and bowling_team in outcomes:
                    bookmaker_odds[bm["key"]] = outcomes

        for pref in BOOKMAKER_PRIORITY:
            if pref in bookmaker_odds:
                return bookmaker_odds[pref], pref

        if bookmaker_odds:
            key = next(iter(bookmaker_odds))
            return bookmaker_odds[key], key

    return None, None


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

    existing_rows = []
    done_keys = set()
    if os.path.exists(OUTPUT_PATH):
        with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
            existing_rows = list(csv.DictReader(f))
        done_keys = {(r["match_id"], r["innings_number"]) for r in existing_rows}

    match_innings_groups = defaultdict(list)
    for s in snapshots:
        key = (s["match_id"], s["innings_number"])
        if key not in done_keys:
            match_innings_groups[key].append(s)

    sorted_groups = sorted(match_innings_groups.keys(),
                           key=lambda k: match_innings_groups[k][0]["date"],
                           reverse=True)

    print(f"\n=== fetch_inplay_pinnacle_pilot.py (optimized) ===")
    print(f"  Total high-confidence snapshots : {len(snapshots)}")
    print(f"  Unique match-innings groups     : {len(sorted_groups)}")
    print(f"  Strategy: 1 API call per match-innings (representative over)")
    print(f"  Already fetched (resuming)     : {len(done_keys)} groups ({len(existing_rows)} rows)")
    print(f"  Bookmaker priority: {', '.join(BOOKMAKER_PRIORITY[:4])}")

    rows = list(existing_rows)
    api_calls = 0
    found = 0
    not_found = 0
    bookmaker_counts = defaultdict(int)

    for key in sorted_groups:
        if api_calls >= MAX_API_CALLS:
            print(f"  Reached API call limit ({MAX_API_CALLS})")
            break

        mid, inn = key
        group_snaps = match_innings_groups[key]
        sample = group_snaps[0]
        date_str = sample["date"]
        bat = sample["batting_team"]
        bowl = sample["bowling_team"]

        start_hour = determine_match_slot(mid, date_str, metadata_by_date)

        overs = sorted(set(int(s["over_number"]) for s in group_snaps))
        median_over = overs[len(overs) // 2]

        ts = estimate_timestamp(date_str, inn, median_over, start_hour)

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

        odds, bm_used = find_best_bookmaker_for_teams(events, bat, bowl)

        if odds and bat in odds and bowl in odds:
            bat_odds = odds[bat]
            bowl_odds = odds[bowl]
            bat_imp = 1.0 / bat_odds
            bowl_imp = 1.0 / bowl_odds
            total_imp = bat_imp + bowl_imp
            bat_market_prob = bat_imp / total_imp
            bowl_market_prob = bowl_imp / total_imp

            bookmaker_counts[bm_used] += 1

            for snap in group_snaps:
                model_prob = float(snap["final_stabilized_probability"])
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
                    "bookmaker_used": bm_used,
                })
            found += len(group_snaps)
            print(f"  Match {mid} inn {inn} ({date_str}): {bm_used} | "
                  f"bat={bat_odds:.2f} bowl={bowl_odds:.2f} | {len(group_snaps)} snaps")
        else:
            not_found += len(group_snaps)
            print(f"  Match {mid} inn {inn} ({date_str}): NO odds found | {len(group_snaps)} snaps skipped")

        if api_calls % 50 == 0 and api_calls > 0:
            save_rows(rows)
            print(f"  Progress: {api_calls} API calls, {found} found, {not_found} not found")

    save_rows(rows)

    print(f"\n  Results:")
    print(f"    API calls              : {api_calls}")
    print(f"    Snapshots with odds    : {found}")
    print(f"    Snapshots without odds : {not_found}")
    print(f"    Total rows in output   : {len(rows)}")
    print(f"    Bookmaker breakdown    :")
    for bm, cnt in sorted(bookmaker_counts.items(), key=lambda x: -x[1]):
        print(f"      {bm}: {cnt} match-innings groups")
    print(f"    Output                 : {OUTPUT_PATH}")


if __name__ == "__main__":
    fetch_inplay_pilot()
