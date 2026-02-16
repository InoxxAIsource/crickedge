import csv
import json
import os
import sys
import time
import requests


PROCESSED_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "processed")
METADATA_PATH = os.path.join(PROCESSED_DIR, "match_metadata.csv")
OUTPUT_PATH = os.path.join(PROCESSED_DIR, "historical_odds_raw.csv")
DATES_CACHE_PATH = os.path.join(PROCESSED_DIR, "odds_fetch_dates_done.json")

API_KEY = os.environ.get("ODDS_API_KEY", "")
BASE_URL = "https://api.the-odds-api.com/v4/historical/sports/cricket_ipl/odds"
BOOKMAKER = "pinnacle"
REGION = "eu"
MARKET = "h2h"
ODDS_FORMAT = "decimal"
SLEEP_BETWEEN_CALLS = 0.5

OUTPUT_FIELDS = [
    "match_id", "date", "team_1", "team_2",
    "bookmaker_name", "team_1_odds", "team_2_odds", "snapshot_timestamp"
]

TEAM_NAME_MAP = {
    "Royal Challengers Bangalore": "Royal Challengers Bengaluru",
    "Delhi Daredevils": "Delhi Capitals",
    "Kings XI Punjab": "Punjab Kings",
    "Rising Pune Supergiants": "Rising Pune Supergiant",
}


def normalize_team(name):
    return TEAM_NAME_MAP.get(name, name)


def load_matches(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_existing_results():
    if not os.path.exists(OUTPUT_PATH):
        return set(), []
    with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    existing_ids = {r["match_id"] for r in rows}
    return existing_ids, rows


def load_dates_done():
    if not os.path.exists(DATES_CACHE_PATH):
        return set()
    with open(DATES_CACHE_PATH, "r") as f:
        return set(json.load(f))


def save_dates_done(dates_done):
    with open(DATES_CACHE_PATH, "w") as f:
        json.dump(sorted(dates_done), f)


def fetch_odds_for_date(date_str):
    snapshot_time = f"{date_str}T13:30:00Z"
    params = {
        "apiKey": API_KEY,
        "regions": REGION,
        "markets": MARKET,
        "oddsFormat": ODDS_FORMAT,
        "date": snapshot_time,
    }
    resp = requests.get(BASE_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json(), snapshot_time


def find_pinnacle_odds(events_data, team_1, team_2):
    for event in events_data:
        event_home = normalize_team(event.get("home_team", ""))
        event_away = normalize_team(event.get("away_team", ""))

        teams_match = (
            (event_home == team_1 and event_away == team_2) or
            (event_home == team_2 and event_away == team_1)
        )
        if not teams_match:
            continue

        for bm in event.get("bookmakers", []):
            if bm["key"] != BOOKMAKER:
                continue
            for market in bm.get("markets", []):
                if market["key"] != MARKET:
                    continue
                outcomes = {normalize_team(o["name"]): o["price"] for o in market["outcomes"]}
                t1_odds = outcomes.get(team_1)
                t2_odds = outcomes.get(team_2)
                if t1_odds and t2_odds:
                    return bm["title"], t1_odds, t2_odds
    return None, None, None


def save_rows(rows):
    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def fetch_historical_odds():
    if not API_KEY:
        print("ERROR: ODDS_API_KEY environment variable not set.", file=sys.stderr)
        sys.exit(1)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    matches = load_matches(METADATA_PATH)

    matches_2020_plus = [m for m in matches if m["date"] >= "2020-01-01"]
    matches_2020_plus.sort(key=lambda m: m["date"])

    existing_ids, rows = load_existing_results()
    dates_done = load_dates_done()

    all_dates = sorted(set(m["date"] for m in matches_2020_plus))
    remaining_dates = sorted(set(all_dates) - dates_done)

    print(f"\n=== fetch_historical_odds.py ===")
    print(f"  Total matches (2020+)      : {len(matches_2020_plus)}")
    print(f"  Total unique dates         : {len(all_dates)}")
    print(f"  Already fetched (matches)  : {len(existing_ids)}")
    print(f"  Dates already processed    : {len(dates_done)}")
    print(f"  Remaining dates to fetch   : {len(remaining_dates)}")
    print(f"  Bookmaker filter           : {BOOKMAKER}")

    if not remaining_dates:
        print("  All dates already fetched. Nothing to do.")
        return rows

    found_this_run = 0
    skipped_no_pinnacle = 0
    skipped_no_match = 0
    api_errors = 0
    api_calls = 0

    for date_str in remaining_dates:
        try:
            data, snapshot_ts = fetch_odds_for_date(date_str)
            events_data = data.get("data", [])
            api_calls += 1
            print(f"  API call #{api_calls}: date={date_str} events={len(events_data)}")
            time.sleep(SLEEP_BETWEEN_CALLS)
        except requests.exceptions.HTTPError as e:
            print(f"  HTTP error for date {date_str}: {e}", file=sys.stderr)
            api_errors += 1
            time.sleep(SLEEP_BETWEEN_CALLS)
            continue
        except Exception as e:
            print(f"  Error for date {date_str}: {e}", file=sys.stderr)
            api_errors += 1
            time.sleep(SLEEP_BETWEEN_CALLS)
            continue

        date_matches = [m for m in matches_2020_plus
                        if m["date"] == date_str and m["match_id"] not in existing_ids]

        for match in date_matches:
            team_1 = match["team_1"]
            team_2 = match["team_2"]
            match_id = match["match_id"]

            bm_name, t1_odds, t2_odds = find_pinnacle_odds(events_data, team_1, team_2)

            if t1_odds is not None and t2_odds is not None:
                row = {
                    "match_id": match_id,
                    "date": date_str,
                    "team_1": team_1,
                    "team_2": team_2,
                    "bookmaker_name": bm_name,
                    "team_1_odds": t1_odds,
                    "team_2_odds": t2_odds,
                    "snapshot_timestamp": snapshot_ts,
                }
                rows.append(row)
                existing_ids.add(match_id)
                found_this_run += 1
            else:
                if events_data:
                    skipped_no_pinnacle += 1
                else:
                    skipped_no_match += 1

        dates_done.add(date_str)
        save_rows(rows)
        save_dates_done(dates_done)

    print(f"\n  Results:")
    print(f"    Total matches with Pinnacle odds  : {len(rows)}")
    print(f"    Found this run                    : {found_this_run}")
    print(f"    Skipped (no Pinnacle)             : {skipped_no_pinnacle}")
    print(f"    Skipped (no match data)           : {skipped_no_match}")
    print(f"    API errors                        : {api_errors}")
    print(f"    API calls made (this run)         : {api_calls}")
    print(f"    Dates processed total             : {len(dates_done)}")
    print(f"    Output                            : {OUTPUT_PATH}")

    return rows


if __name__ == "__main__":
    fetch_historical_odds()
