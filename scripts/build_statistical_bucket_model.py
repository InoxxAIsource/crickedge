import csv
import os
import sys
from collections import defaultdict


PROCESSED_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "processed")
INPUT_PATH = os.path.join(PROCESSED_DIR, "over_state_snapshots_enriched.csv")
OUTPUT_PATH = os.path.join(PROCESSED_DIR, "statistical_bucket_model.csv")

OUTPUT_FIELDS = [
    "over_bucket", "wickets_bucket", "run_pressure_bucket", "elo_diff_bucket",
    "total_samples", "batting_team_wins", "win_probability"
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


def get_statistical_win_probability(state_row):
    ob = over_bucket(state_row["over_number"])
    wb = wickets_bucket(state_row["wickets_so_far"])
    rpb = run_pressure_bucket(state_row)
    edb = elo_diff_bucket(state_row.get("elo_rating_difference", ""))
    return ob, wb, rpb, edb


def build_statistical_bucket_model():
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    with open(INPUT_PATH, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    bucket_counts = defaultdict(lambda: {"total": 0, "wins": 0})

    for row in rows:
        ob, wb, rpb, edb = get_statistical_win_probability(row)
        key = (ob, wb, rpb, edb)

        bucket_counts[key]["total"] += 1

        batting_team = row["batting_team"]
        eventual_winner = row["eventual_winner"]
        if batting_team == eventual_winner:
            bucket_counts[key]["wins"] += 1

    output_rows = []
    for (ob, wb, rpb, edb), stats in sorted(bucket_counts.items()):
        win_prob = round(stats["wins"] / stats["total"], 4) if stats["total"] > 0 else 0.0
        output_rows.append({
            "over_bucket": ob,
            "wickets_bucket": wb,
            "run_pressure_bucket": rpb,
            "elo_diff_bucket": edb,
            "total_samples": stats["total"],
            "batting_team_wins": stats["wins"],
            "win_probability": win_prob,
        })

    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(output_rows)

    print("\n=== build_statistical_bucket_model.py Summary ===")
    print(f"  Total snapshot rows processed  : {len(rows)}")
    print(f"  Unique bucket combinations     : {len(output_rows)}")
    print(f"  Output                         : {OUTPUT_PATH}")

    first_inn = [r for r in output_rows if r["run_pressure_bucket"] == "first_innings"]
    second_inn = [r for r in output_rows if r["run_pressure_bucket"] != "first_innings"]
    print(f"  First innings buckets          : {len(first_inn)}")
    print(f"  Second innings buckets         : {len(second_inn)}")

    print(f"\n  Sample rows (highest sample count):")
    top = sorted(output_rows, key=lambda r: r["total_samples"], reverse=True)[:5]
    for r in top:
        print(f"    over={r['over_bucket']:5s} wkt={r['wickets_bucket']:3s} "
              f"pressure={r['run_pressure_bucket']:14s} elo={r['elo_diff_bucket']:24s} "
              f"n={r['total_samples']:5d} win_prob={r['win_probability']:.4f}")

    return output_rows


if __name__ == "__main__":
    build_statistical_bucket_model()
