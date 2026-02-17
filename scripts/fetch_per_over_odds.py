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
OUTPUT_PATH = os.path.join(PROCESSED_DIR, "per_over_aligned_odds.csv")
CACHE_PATH = os.path.join(PROCESSED_DIR, "per_over_odds_cache.json")

API_KEY = os.environ.get("ODDS_API_KEY", "")
BASE_URL = "https://api.the-odds-api.com/v4/historical/sports/cricket_ipl/odds"
REGION = "eu"
MARKET = "h2h"
ODDS_FORMAT = "decimal"
SLEEP_BETWEEN_CALLS = 0.3
MAX_API_CALLS = 1500

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


def estimate_over_timestamp(date_str, innings_number, over_number, start_hour):
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


def load_cache():
    if os.path.exists(CACHE_PATH):
        with open(CACHE_PATH, "r") as f:
            return json.load(f)
    return {}


def save_cache(cache):
    with open(CACHE_PATH, "w") as f:
        json.dump(cache, f)


def fetch_odds_snapshot(timestamp, cache):
    if timestamp in cache:
        return cache[timestamp], False

    params = {
        "apiKey": API_KEY,
        "regions": REGION,
        "markets": MARKET,
        "oddsFormat": ODDS_FORMAT,
        "date": timestamp,
    }
    resp = requests.get(BASE_URL, params=params, timeout=30)
    resp.raise_for_status()
    result = resp.json()
    cache[timestamp] = result
    return result, True


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


def fetch_per_over():
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

    done_keys = set()
    existing_rows = []
    if os.path.exists(OUTPUT_PATH):
        with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
            existing_rows = list(csv.DictReader(f))
        done_keys = {(r["match_id"], r["innings_number"], r["over_number"]) for r in existing_rows}

    todo = []
    for s in snapshots:
        key = (s["match_id"], s["innings_number"], s["over_number"])
        if key not in done_keys:
            todo.append(s)

    todo.sort(key=lambda s: (s["date"], s["match_id"], int(s["innings_number"]), int(s["over_number"])))

    print(f"\n=== fetch_per_over_odds.py ===")
    print(f"  Total high-confidence snapshots : {len(snapshots)}")
    print(f"  Already fetched (resume)        : {len(done_keys)}")
    print(f"  Remaining to fetch              : {len(todo)}")
    print(f"  Strategy: 1 API call per unique timestamp (per-over aligned)")
    print(f"  Bookmaker priority: {', '.join(BOOKMAKER_PRIORITY[:4])}")

    cache = load_cache()
    rows = list(existing_rows)
    api_calls = 0
    cache_hits = 0
    found = 0
    not_found = 0
    bookmaker_counts = defaultdict(int)

    for i, snap in enumerate(todo):
        if api_calls >= MAX_API_CALLS:
            print(f"\n  Reached API call limit ({MAX_API_CALLS})")
            break

        mid = snap["match_id"]
        inn = snap["innings_number"]
        over = snap["over_number"]
        date_str = snap["date"]
        bat = snap["batting_team"]
        bowl = snap["bowling_team"]

        start_hour = determine_match_slot(mid, date_str, metadata_by_date)
        ts = estimate_over_timestamp(date_str, inn, over, start_hour)

        try:
            data, was_api_call = fetch_odds_snapshot(ts, cache)
            events = data.get("data", [])
            if was_api_call:
                api_calls += 1
                time.sleep(SLEEP_BETWEEN_CALLS)
            else:
                cache_hits += 1
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

            model_prob = float(snap["final_stabilized_probability"])
            edge = model_prob - bat_market_prob

            rows.append({
                "match_id": mid,
                "date": date_str,
                "innings_number": inn,
                "over_number": over,
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
            found += 1

            if found % 50 == 0:
                print(f"  Progress: {found} found, {not_found} missed, "
                      f"{api_calls} API calls, {cache_hits} cache hits")
        else:
            not_found += 1

        if (api_calls + cache_hits) % 100 == 0 and (api_calls + cache_hits) > 0:
            save_rows(rows)
            save_cache(cache)

    save_rows(rows)
    save_cache(cache)

    print(f"\n  Results:")
    print(f"    API calls              : {api_calls}")
    print(f"    Cache hits             : {cache_hits}")
    print(f"    Snapshots with odds    : {found}")
    print(f"    Snapshots without odds : {not_found}")
    print(f"    Total rows in output   : {len(rows)}")
    print(f"    Bookmaker breakdown    :")
    for bm, cnt in sorted(bookmaker_counts.items(), key=lambda x: -x[1]):
        print(f"      {bm}: {cnt} snapshots")
    print(f"    Output                 : {OUTPUT_PATH}")
    print(f"    Cache                  : {CACHE_PATH}")


if __name__ == "__main__":
    fetch_per_over()
