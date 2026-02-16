import json
import csv
import os
import sys


DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw_json")
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "processed", "powerplay_summary.csv")

FIELDNAMES = [
    "match_id", "season", "venue", "batting_team", "bowling_team",
    "toss_winner", "toss_decision", "powerplay_runs", "powerplay_wickets",
    "final_winner"
]


TEAM_NAME_MAP = {
    "Royal Challengers Bangalore": "Royal Challengers Bengaluru",
    "Delhi Daredevils": "Delhi Capitals",
    "Kings XI Punjab": "Punjab Kings",
    "Rising Pune Supergiants": "Rising Pune Supergiant",
}


def normalize_team(name):
    return TEAM_NAME_MAP.get(name, name)


def load_json(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def is_valid_match(info):
    outcome = info.get("outcome", {})
    if "winner" not in outcome:
        return False
    if "method" in outcome:
        return False
    return True


def compute_powerplay(overs_data):
    runs = 0
    wickets = 0
    for over_obj in overs_data:
        over_num = over_obj["over"]
        if over_num >= 6:
            break
        for delivery in over_obj["deliveries"]:
            runs += delivery["runs"]["total"]
            if "wickets" in delivery:
                wickets += len(delivery["wickets"])
    return runs, wickets


def extract_powerplay(match_id, data):
    info = data["info"]
    innings_list = data.get("innings", [])
    if not innings_list:
        return None

    first_innings = innings_list[0]
    batting_team = first_innings["team"]
    teams = info.get("teams", [])
    bowling_team = [t for t in teams if t != batting_team]
    bowling_team = bowling_team[0] if bowling_team else ""

    pp_runs, pp_wickets = compute_powerplay(first_innings.get("overs", []))

    return {
        "match_id": match_id,
        "season": str(info.get("season", "")),
        "venue": info.get("venue", ""),
        "batting_team": normalize_team(batting_team),
        "bowling_team": normalize_team(bowling_team),
        "toss_winner": normalize_team(info.get("toss", {}).get("winner", "")),
        "toss_decision": info.get("toss", {}).get("decision", ""),
        "powerplay_runs": pp_runs,
        "powerplay_wickets": pp_wickets,
        "final_winner": normalize_team(info["outcome"]["winner"]),
    }


def build_powerplay_summary():
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    json_files = sorted([f for f in os.listdir(DATA_DIR) if f.endswith(".json")])

    rows = []
    skipped = 0
    errors = 0

    for filename in json_files:
        filepath = os.path.join(DATA_DIR, filename)
        match_id = filename.replace(".json", "")
        try:
            data = load_json(filepath)
            info = data["info"]

            if not is_valid_match(info):
                skipped += 1
                continue

            row = extract_powerplay(match_id, data)
            if row:
                rows.append(row)
        except Exception as e:
            errors += 1
            print(f"  Error processing {filename}: {e}", file=sys.stderr)

    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    print("\n=== build_powerplay_summary.py Summary ===")
    print(f"  Total JSON files scanned : {len(json_files)}")
    print(f"  Valid matches saved      : {len(rows)}")
    print(f"  Skipped (invalid)        : {skipped}")
    print(f"  Errors                   : {errors}")
    print(f"  Output                   : {OUTPUT_PATH}")
    if rows:
        avg_runs = sum(r["powerplay_runs"] for r in rows) / len(rows)
        avg_wkts = sum(r["powerplay_wickets"] for r in rows) / len(rows)
        print(f"  Avg powerplay runs       : {avg_runs:.1f}")
        print(f"  Avg powerplay wickets    : {avg_wkts:.1f}")

    return rows


if __name__ == "__main__":
    build_powerplay_summary()
