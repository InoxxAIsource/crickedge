import csv
import os
from collections import defaultdict


PROCESSED_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "processed")
ENRICHED_PATH = os.path.join(PROCESSED_DIR, "over_state_snapshots_enriched.csv")
STABILIZED_PATH = os.path.join(PROCESSED_DIR, "statistical_bucket_model_stabilized.csv")
METADATA_PATH = os.path.join(PROCESSED_DIR, "match_metadata.csv")
OUTPUT_PATH = os.path.join(PROCESSED_DIR, "live_model_calibration_2020_plus.csv")

OUTPUT_FIELDS = [
    "decile", "sample_count", "avg_predicted_prob", "actual_win_rate", "abs_calibration_error"
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


def get_over_phase(over_num):
    over_num = int(over_num)
    if over_num <= 6:
        return "powerplay"
    elif over_num <= 15:
        return "middle"
    else:
        return "death"


def get_decile(prob):
    if prob >= 1.0:
        return "90-100%"
    decile_idx = int(prob * 10)
    lower = decile_idx * 10
    upper = lower + 10
    return f"{lower}-{upper}%"


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
        return {r["match_id"]: r["date"] for r in csv.DictReader(f)}


def live_model_calibration():
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    model_lookup = load_stabilized_model()
    match_dates = load_match_dates()

    with open(ENRICHED_PATH, "r", encoding="utf-8") as f:
        all_rows = list(csv.DictReader(f))

    rows_2020 = [r for r in all_rows if match_dates.get(r["match_id"], "") >= "2020-01-01"]

    print(f"\n=== live_model_calibration.py ===")
    print(f"  Total enriched snapshots   : {len(all_rows)}")
    print(f"  2020+ snapshots            : {len(rows_2020)}")

    decile_data = defaultdict(lambda: {"count": 0, "wins": 0, "sum_pred": 0.0})
    phase_data = defaultdict(lambda: {"count": 0, "sum_brier": 0.0})
    overall_brier_sum = 0.0
    overall_count = 0
    high_prob_counts = {70: 0, 80: 0, 90: 0}
    high_prob_wins = {70: 0, 80: 0, 90: 0}
    no_bucket = 0

    for row in rows_2020:
        ob = over_bucket(row["over_number"])
        wb = wickets_bucket(row["wickets_so_far"])
        rpb = run_pressure_bucket(row)
        edb = elo_diff_bucket(row.get("elo_rating_difference", ""))

        key = (ob, wb, rpb, edb)
        prob = model_lookup.get(key)
        if prob is None:
            no_bucket += 1
            continue

        batting_team = row["batting_team"]
        winner = row["eventual_winner"]
        batting_won = 1 if batting_team == winner else 0

        brier = (prob - batting_won) ** 2

        decile = get_decile(prob)
        decile_data[decile]["count"] += 1
        decile_data[decile]["wins"] += batting_won
        decile_data[decile]["sum_pred"] += prob

        phase = get_over_phase(row["over_number"])
        phase_data[phase]["count"] += 1
        phase_data[phase]["sum_brier"] += brier

        overall_brier_sum += brier
        overall_count += 1

        prob_pct = prob * 100
        for threshold in [70, 80, 90]:
            if prob_pct >= threshold:
                high_prob_counts[threshold] += 1
                high_prob_wins[threshold] += batting_won

    overall_brier = overall_brier_sum / overall_count if overall_count > 0 else 0

    decile_order = ["0-10%", "10-20%", "20-30%", "30-40%", "40-50%",
                    "50-60%", "60-70%", "70-80%", "80-90%", "90-100%"]

    output_rows = []
    for d in decile_order:
        dd = decile_data[d]
        count = dd["count"]
        if count > 0:
            avg_pred = dd["sum_pred"] / count
            actual_wr = dd["wins"] / count
            cal_err = abs(actual_wr - avg_pred)
        else:
            avg_pred = 0
            actual_wr = 0
            cal_err = 0
        output_rows.append({
            "decile": d,
            "sample_count": count,
            "avg_predicted_prob": round(avg_pred, 4),
            "actual_win_rate": round(actual_wr, 4),
            "abs_calibration_error": round(cal_err, 4),
        })

    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(output_rows)

    print(f"\n  Calibration by Decile:")
    print(f"  {'Decile':>10s} {'Count':>8s} {'AvgPred':>8s} {'ActWin%':>8s} {'CalErr':>8s}")
    print(f"  {'-'*10} {'-'*8} {'-'*8} {'-'*8} {'-'*8}")
    for r in output_rows:
        print(f"  {r['decile']:>10s} {r['sample_count']:>8d} "
              f"{r['avg_predicted_prob']:>8.4f} {r['actual_win_rate']:>8.4f} "
              f"{r['abs_calibration_error']:>8.4f}")

    print(f"\n  Overall Brier Score (2020+): {overall_brier:.6f}")
    print(f"  Snapshots scored           : {overall_count}")
    print(f"  Buckets not found          : {no_bucket}")

    print(f"\n  Brier Score by Phase:")
    for phase in ["powerplay", "middle", "death"]:
        pd = phase_data[phase]
        if pd["count"] > 0:
            phase_brier = pd["sum_brier"] / pd["count"]
            print(f"    {phase:>10s}: {phase_brier:.6f} ({pd['count']} snapshots)")

    print(f"\n  Sharpness (High-Probability States):")
    for threshold in [70, 80, 90]:
        cnt = high_prob_counts[threshold]
        wins = high_prob_wins[threshold]
        pct = (cnt / overall_count * 100) if overall_count > 0 else 0
        wr = (wins / cnt * 100) if cnt > 0 else 0
        print(f"    >{threshold}%: {cnt:>6d} snapshots ({pct:.1f}% of total), "
              f"actual win rate: {wr:.1f}%")

    print(f"\n  Output: {OUTPUT_PATH}")


if __name__ == "__main__":
    live_model_calibration()
