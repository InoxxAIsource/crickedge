import csv
import os
from collections import defaultdict


PROCESSED_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "processed")
ENRICHED_PATH = os.path.join(PROCESSED_DIR, "over_state_snapshots_enriched.csv")
STABILIZED_PATH = os.path.join(PROCESSED_DIR, "statistical_bucket_model_stabilized.csv")
METADATA_PATH = os.path.join(PROCESSED_DIR, "match_metadata.csv")
OUTPUT_PATH = os.path.join(PROCESSED_DIR, "high_confidence_snapshots_85_plus.csv")

THRESHOLD = 0.85

OUTPUT_FIELDS = [
    "match_id", "season", "date", "innings_number", "over_number",
    "batting_team", "bowling_team", "final_stabilized_probability",
    "eventual_winner"
]


def over_bucket(over_num):
    over_num = int(over_num)
    if over_num <= 3:
        return "1-3"
    elif over_num <= 6:
        return "4-6"
    elif over_num <= 10:
        return "7-10"
    elif over_num <= 15:
        return "11-15"
    else:
        return "16-20"


def wickets_bucket(wickets):
    wickets = int(wickets)
    if wickets <= 1:
        return "0-1"
    elif wickets <= 3:
        return "2-3"
    elif wickets <= 6:
        return "4-6"
    else:
        return "7+"


def run_pressure_bucket(row):
    innings = int(row["innings_number"])
    if innings == 1:
        return "first_innings"

    target = row.get("target", "")
    balls_rem = row.get("balls_remaining", "")
    runs_so_far = row.get("runs_so_far", "")
    over_num = row.get("over_number", "")

    if target == "" or balls_rem == "" or runs_so_far == "" or over_num == "":
        return "first_innings"

    target = float(target)
    balls_rem = float(balls_rem)
    runs_so_far = float(runs_so_far)
    over_num = float(over_num)

    if over_num > 0:
        current_rr = (runs_so_far / (over_num * 6)) * 6
    else:
        current_rr = 0.0

    rrr_val = float(row.get("required_run_rate", 0))
    pressure = rrr_val - current_rr

    if pressure <= -4:
        return "very_low"
    elif pressure <= -1:
        return "low"
    elif pressure <= 1:
        return "neutral"
    elif pressure <= 4:
        return "high"
    else:
        return "very_high"


def elo_diff_bucket(elo_diff):
    if elo_diff == "":
        return "neutral"
    elo_diff = float(elo_diff)
    if elo_diff > 75:
        return "strong_advantage"
    elif elo_diff > 25:
        return "moderate_advantage"
    elif elo_diff >= -25:
        return "neutral"
    elif elo_diff >= -75:
        return "moderate_disadvantage"
    else:
        return "strong_disadvantage"


def load_stabilized_model():
    with open(STABILIZED_PATH, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    lookup = {}
    for r in rows:
        key = (r["over_bucket"], r["wickets_bucket"], r["run_pressure_bucket"], r["elo_diff_bucket"])
        lookup[key] = float(r["final_stabilized_probability"])
    return lookup


def load_match_dates():
    with open(METADATA_PATH, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    lookup = {}
    for r in rows:
        lookup[r["match_id"]] = {"date": r["date"], "season": r["season"]}
    return lookup


def extract_high_confidence():
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    model_lookup = load_stabilized_model()
    match_info = load_match_dates()

    with open(ENRICHED_PATH, "r", encoding="utf-8") as f:
        all_rows = list(csv.DictReader(f))

    output_rows = []
    total_2020 = 0

    for row in all_rows:
        mid = row["match_id"]
        info = match_info.get(mid, {})
        date = info.get("date", "")
        if date < "2020-01-01":
            continue
        total_2020 += 1

        ob = over_bucket(row["over_number"])
        wb = wickets_bucket(row["wickets_so_far"])
        rpb = run_pressure_bucket(row)
        edb = elo_diff_bucket(row.get("elo_rating_difference", ""))

        key = (ob, wb, rpb, edb)
        prob = model_lookup.get(key)
        if prob is None:
            continue

        if prob >= THRESHOLD:
            output_rows.append({
                "match_id": mid,
                "season": info.get("season", row.get("season", "")),
                "date": date,
                "innings_number": row["innings_number"],
                "over_number": row["over_number"],
                "batting_team": row["batting_team"],
                "bowling_team": row["bowling_team"],
                "final_stabilized_probability": round(prob, 6),
                "eventual_winner": row["eventual_winner"],
            })

    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(output_rows)

    unique_matches = len(set(r["match_id"] for r in output_rows))
    avg_prob = (sum(float(r["final_stabilized_probability"]) for r in output_rows) / len(output_rows)
                if output_rows else 0)

    print(f"\n=== extract_high_confidence_snapshots.py ===")
    print(f"  Total 2020+ snapshots      : {total_2020}")
    print(f"  Snapshots >= {THRESHOLD:.0%}        : {len(output_rows)}")
    print(f"  Unique matches involved    : {unique_matches}")
    print(f"  Avg probability            : {avg_prob:.4f}")
    print(f"  Output                     : {OUTPUT_PATH}")


if __name__ == "__main__":
    extract_high_confidence()
