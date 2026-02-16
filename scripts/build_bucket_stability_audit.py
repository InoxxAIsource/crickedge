import csv
import math
import os


PROCESSED_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "processed")
INPUT_PATH = os.path.join(PROCESSED_DIR, "statistical_bucket_model.csv")
OUTPUT_PATH = os.path.join(PROCESSED_DIR, "statistical_bucket_model_with_stability.csv")

OUTPUT_FIELDS = [
    "over_bucket", "wickets_bucket", "run_pressure_bucket", "elo_diff_bucket",
    "sample_size", "batting_team_wins", "win_probability",
    "standard_error", "ci_lower_95", "ci_upper_95"
]


def build_bucket_stability_audit():
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    with open(INPUT_PATH, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    output_rows = []
    unstable = 0
    moderate = 0
    max_sample = 0
    min_sample = float("inf")

    for row in rows:
        n = int(row["total_samples"])
        p = float(row["win_probability"])

        if n > 0:
            se = round(math.sqrt(p * (1 - p) / n), 6)
        else:
            se = 0.0

        ci_lower = round(max(0.0, p - 1.96 * se), 4)
        ci_upper = round(min(1.0, p + 1.96 * se), 4)

        if n < 30:
            unstable += 1
        elif n < 50:
            moderate += 1

        max_sample = max(max_sample, n)
        min_sample = min(min_sample, n)

        output_rows.append({
            "over_bucket": row["over_bucket"],
            "wickets_bucket": row["wickets_bucket"],
            "run_pressure_bucket": row["run_pressure_bucket"],
            "elo_diff_bucket": row["elo_diff_bucket"],
            "sample_size": n,
            "batting_team_wins": row["batting_team_wins"],
            "win_probability": p,
            "standard_error": se,
            "ci_lower_95": ci_lower,
            "ci_upper_95": ci_upper,
        })

    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(output_rows)

    print("\n=== build_bucket_stability_audit.py Summary ===")
    print(f"  Total buckets              : {len(output_rows)}")
    print(f"  Unstable (< 30 samples)    : {unstable}")
    print(f"  Moderate (30–50 samples)   : {moderate}")
    print(f"  Stable (>= 50 samples)     : {len(output_rows) - unstable - moderate}")
    print(f"  Largest bucket sample size  : {max_sample}")
    print(f"  Smallest bucket sample size : {min_sample}")
    print(f"  Output                      : {OUTPUT_PATH}")

    print(f"\n  Top 5 most unstable buckets (smallest sample):")
    by_size = sorted(output_rows, key=lambda r: r["sample_size"])
    for r in by_size[:5]:
        print(f"    over={r['over_bucket']:5s} wkt={r['wickets_bucket']:3s} "
              f"pressure={r['run_pressure_bucket']:14s} elo={r['elo_diff_bucket']:24s} "
              f"n={r['sample_size']:4d} win_prob={r['win_probability']:.4f} "
              f"CI=[{r['ci_lower_95']:.4f}, {r['ci_upper_95']:.4f}]")

    return output_rows


if __name__ == "__main__":
    build_bucket_stability_audit()
