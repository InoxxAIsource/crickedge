import json
import csv
import os
import sys


DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw_json")
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "processed", "over_state_snapshots.csv")

FIELDNAMES = [
    "match_id", "season", "venue", "innings_number", "over_number",
    "batting_team", "bowling_team", "runs_so_far", "wickets_so_far",
    "balls_remaining", "target", "required_run_rate", "eventual_winner"
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


def count_legal_deliveries(over_obj):
    count = 0
    for delivery in over_obj["deliveries"]:
        extras = delivery.get("extras", {})
        if "wides" not in extras and "noballs" not in extras:
            count += 1
    return count


def get_innings_total_balls(innings_obj):
    total = 0
    for over_obj in innings_obj.get("overs", []):
        total += count_legal_deliveries(over_obj)
    return total


def compute_over_snapshots(match_id, data):
    info = data["info"]
    innings_list = data.get("innings", [])
    teams = info.get("teams", [])
    winner = normalize_team(info["outcome"]["winner"])
    season = str(info.get("season", ""))
    venue = info.get("venue", "")
    max_overs = info.get("overs", 20)

    first_innings_total = None
    second_innings_max_balls = None
    snapshots = []

    if len(innings_list) >= 2:
        second_innings_max_balls = max_overs * 6

    for innings_idx, innings_obj in enumerate(innings_list):
        innings_number = innings_idx + 1
        batting_team = normalize_team(innings_obj["team"])
        bowling_team = [normalize_team(t) for t in teams if normalize_team(t) != batting_team]
        bowling_team = bowling_team[0] if bowling_team else ""

        cumulative_runs = 0
        cumulative_wickets = 0
        cumulative_balls = 0

        for over_obj in innings_obj.get("overs", []):
            over_num = over_obj["over"]
            over_runs = 0
            over_wickets = 0
            legal_balls = count_legal_deliveries(over_obj)

            for delivery in over_obj["deliveries"]:
                over_runs += delivery["runs"]["total"]
                if "wickets" in delivery:
                    over_wickets += len(delivery["wickets"])

            cumulative_runs += over_runs
            cumulative_wickets += over_wickets
            cumulative_balls += legal_balls

            completed_over = over_num + 1

            if innings_number == 2 and first_innings_total is not None:
                target = first_innings_total + 1
                balls_remaining = second_innings_max_balls - cumulative_balls
                if balls_remaining > 0:
                    runs_needed = target - cumulative_runs
                    rrr = round((runs_needed / balls_remaining) * 6, 2)
                else:
                    rrr = 0.0
            else:
                target = ""
                balls_remaining = ""
                rrr = ""

            snapshots.append({
                "match_id": match_id,
                "season": season,
                "venue": venue,
                "innings_number": innings_number,
                "over_number": completed_over,
                "batting_team": batting_team,
                "bowling_team": bowling_team,
                "runs_so_far": cumulative_runs,
                "wickets_so_far": cumulative_wickets,
                "balls_remaining": balls_remaining,
                "target": target,
                "required_run_rate": rrr,
                "eventual_winner": winner,
            })

        if innings_number == 1:
            first_innings_total = cumulative_runs

    return snapshots


def build_over_state_snapshots():
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    json_files = sorted([f for f in os.listdir(DATA_DIR) if f.endswith(".json")])

    all_rows = []
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

            snapshots = compute_over_snapshots(match_id, data)
            all_rows.extend(snapshots)
        except Exception as e:
            errors += 1
            print(f"  Error processing {filename}: {e}", file=sys.stderr)

    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(all_rows)

    print("\n=== build_over_state_snapshots.py Summary ===")
    print(f"  Total JSON files scanned   : {len(json_files)}")
    print(f"  Valid matches processed     : {len(json_files) - skipped - errors}")
    print(f"  Skipped (invalid)           : {skipped}")
    print(f"  Errors                      : {errors}")
    print(f"  Total snapshot rows saved   : {len(all_rows)}")
    print(f"  Output                      : {OUTPUT_PATH}")
    if all_rows:
        innings_1 = [r for r in all_rows if r["innings_number"] == 1]
        innings_2 = [r for r in all_rows if r["innings_number"] == 2]
        print(f"  Innings 1 rows              : {len(innings_1)}")
        print(f"  Innings 2 rows              : {len(innings_2)}")

    return all_rows


if __name__ == "__main__":
    build_over_state_snapshots()
