import json
import csv
import os
import sys


DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw_json")
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "processed", "match_metadata.csv")

FIELDNAMES = [
    "match_id", "season", "date", "venue",
    "team_1", "team_2", "toss_winner", "toss_decision", "winner"
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


def extract_metadata(match_id, data):
    info = data["info"]
    teams = info.get("teams", [])
    return {
        "match_id": match_id,
        "season": str(info.get("season", "")),
        "date": info.get("dates", [""])[0],
        "venue": info.get("venue", ""),
        "team_1": normalize_team(teams[0]) if len(teams) > 0 else "",
        "team_2": normalize_team(teams[1]) if len(teams) > 1 else "",
        "toss_winner": normalize_team(info.get("toss", {}).get("winner", "")),
        "toss_decision": info.get("toss", {}).get("decision", ""),
        "winner": normalize_team(info["outcome"]["winner"]),
    }


def build_metadata():
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    json_files = sorted([f for f in os.listdir(DATA_DIR) if f.endswith(".json")])

    rows = []
    skipped_no_winner = 0
    skipped_dls = 0
    errors = 0

    for filename in json_files:
        filepath = os.path.join(DATA_DIR, filename)
        match_id = filename.replace(".json", "")
        try:
            data = load_json(filepath)
            info = data["info"]
            outcome = info.get("outcome", {})

            if "winner" not in outcome:
                skipped_no_winner += 1
                continue
            if "method" in outcome:
                skipped_dls += 1
                continue

            row = extract_metadata(match_id, data)
            rows.append(row)
        except Exception as e:
            errors += 1
            print(f"  Error processing {filename}: {e}", file=sys.stderr)

    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    unique_teams = sorted(set(
        [r["team_1"] for r in rows] + [r["team_2"] for r in rows]
    ))

    print("\n=== build_metadata.py Summary ===")
    print(f"  Total JSON files scanned : {len(json_files)}")
    print(f"  Valid matches saved      : {len(rows)}")
    print(f"  Skipped (no winner)      : {skipped_no_winner}")
    print(f"  Skipped (DLS method)     : {skipped_dls}")
    print(f"  Errors                   : {errors}")
    print(f"  Unique teams             : {len(unique_teams)}")
    print(f"  Output                   : {OUTPUT_PATH}")
    print(f"\n  Team list after normalization:")
    for t in unique_teams:
        print(f"    - {t}")

    return rows


if __name__ == "__main__":
    build_metadata()
