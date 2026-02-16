import csv
import os


PROCESSED_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "processed")
INPUT_PATH = os.path.join(PROCESSED_DIR, "statistical_bucket_model_with_stability.csv")
OUTPUT_PATH = os.path.join(PROCESSED_DIR, "statistical_bucket_model_stabilized.csv")

MIN_SAMPLE = 50

OUTPUT_FIELDS = [
    "over_bucket", "wickets_bucket", "run_pressure_bucket", "elo_diff_bucket",
    "sample_size", "batting_team_wins", "win_probability",
    "standard_error", "ci_lower_95", "ci_upper_95",
    "final_stabilized_probability", "fallback_level"
]


def build_stabilized_model():
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    with open(INPUT_PATH, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    for r in rows:
        r["sample_size"] = int(r["sample_size"])
        r["win_probability"] = float(r["win_probability"])
        r["batting_team_wins"] = int(r["batting_team_wins"])

    level2 = {}
    level3 = {}
    level4 = {}
    global_wins = 0
    global_total = 0

    for r in rows:
        n = r["sample_size"]
        w = r["batting_team_wins"]

        k2 = (r["over_bucket"], r["wickets_bucket"], r["run_pressure_bucket"])
        level2.setdefault(k2, {"total": 0, "wins": 0})
        level2[k2]["total"] += n
        level2[k2]["wins"] += w

        k3 = (r["over_bucket"], r["wickets_bucket"])
        level3.setdefault(k3, {"total": 0, "wins": 0})
        level3[k3]["total"] += n
        level3[k3]["wins"] += w

        k4 = (r["over_bucket"],)
        level4.setdefault(k4, {"total": 0, "wins": 0})
        level4[k4]["total"] += n
        level4[k4]["wins"] += w

        global_total += n
        global_wins += w

    global_prob = global_wins / global_total if global_total > 0 else 0.5

    used_raw = 0
    used_fallback = 0
    fallback_counts = {2: 0, 3: 0, 4: 0, 5: 0}

    output_rows = []
    for r in rows:
        n = r["sample_size"]

        if n >= MIN_SAMPLE:
            final_prob = r["win_probability"]
            level = 1
            used_raw += 1
        else:
            k2 = (r["over_bucket"], r["wickets_bucket"], r["run_pressure_bucket"])
            k3 = (r["over_bucket"], r["wickets_bucket"])
            k4 = (r["over_bucket"],)

            if level2[k2]["total"] >= MIN_SAMPLE:
                final_prob = level2[k2]["wins"] / level2[k2]["total"]
                level = 2
            elif level3[k3]["total"] >= MIN_SAMPLE:
                final_prob = level3[k3]["wins"] / level3[k3]["total"]
                level = 3
            elif level4[k4]["total"] >= MIN_SAMPLE:
                final_prob = level4[k4]["wins"] / level4[k4]["total"]
                level = 4
            else:
                final_prob = global_prob
                level = 5

            used_fallback += 1
            fallback_counts[level] += 1

        output_rows.append({
            "over_bucket": r["over_bucket"],
            "wickets_bucket": r["wickets_bucket"],
            "run_pressure_bucket": r["run_pressure_bucket"],
            "elo_diff_bucket": r["elo_diff_bucket"],
            "sample_size": r["sample_size"],
            "batting_team_wins": r["batting_team_wins"],
            "win_probability": r["win_probability"],
            "standard_error": r["standard_error"],
            "ci_lower_95": r["ci_lower_95"],
            "ci_upper_95": r["ci_upper_95"],
            "final_stabilized_probability": round(final_prob, 4),
            "fallback_level": level,
        })

    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(output_rows)

    print("\n=== build_stabilized_model.py Summary ===")
    print(f"  Total buckets                  : {len(output_rows)}")
    print(f"  Using raw probability (L1)     : {used_raw}")
    print(f"  Using fallback                 : {used_fallback}")
    print(f"    Fallback to Level 2          : {fallback_counts[2]}")
    print(f"    Fallback to Level 3          : {fallback_counts[3]}")
    print(f"    Fallback to Level 4          : {fallback_counts[4]}")
    print(f"    Fallback to Level 5 (global) : {fallback_counts[5]}")
    print(f"  Global baseline win rate       : {global_prob:.4f}")
    print(f"  Output                         : {OUTPUT_PATH}")

    return output_rows


if __name__ == "__main__":
    build_stabilized_model()
