import csv
import math
import os
from collections import defaultdict


PROCESSED_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "processed")
INPUT_PATH = os.path.join(PROCESSED_DIR, "over_state_snapshots_enriched.csv")
OUTPUT_PATH = os.path.join(PROCESSED_DIR, "rolling_backtest_results.csv")

TRAIN_YEARS = 3
MIN_SAMPLE = 50

OUTPUT_FIELDS = [
    "window", "train_start", "train_end", "test_year",
    "train_rows", "test_rows", "test_matches",
    "brier_score", "log_loss", "accuracy", "avg_predicted_prob",
    "calibration_decile_1", "calibration_decile_2", "calibration_decile_3",
    "calibration_decile_4", "calibration_decile_5", "calibration_decile_6",
    "calibration_decile_7", "calibration_decile_8", "calibration_decile_9",
    "calibration_decile_10"
]


def get_year(date_str):
    return int(date_str[:4])


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

    over_num = row.get("over_number", "")
    runs_so_far = row.get("runs_so_far", "")
    rrr_val = row.get("required_run_rate", "")

    if over_num == "" or runs_so_far == "" or rrr_val == "":
        return "first_innings"

    over_num = float(over_num)
    runs_so_far = float(runs_so_far)
    rrr_val = float(rrr_val)

    current_rr = (runs_so_far / (over_num * 6)) * 6 if over_num > 0 else 0.0
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


def bucketize(row):
    ob = over_bucket(row["over_number"])
    wb = wickets_bucket(row["wickets_so_far"])
    rpb = run_pressure_bucket(row)
    edb = elo_diff_bucket(row.get("elo_rating_difference", ""))
    return ob, wb, rpb, edb


def build_model_from_rows(train_rows):
    counts = defaultdict(lambda: {"total": 0, "wins": 0})

    for row in train_rows:
        key = bucketize(row)
        counts[key]["total"] += 1
        if row["batting_team"] == row["eventual_winner"]:
            counts[key]["wins"] += 1

    level2 = defaultdict(lambda: {"total": 0, "wins": 0})
    level3 = defaultdict(lambda: {"total": 0, "wins": 0})
    level4 = defaultdict(lambda: {"total": 0, "wins": 0})
    global_total = 0
    global_wins = 0

    for (ob, wb, rpb, edb), s in counts.items():
        level2[(ob, wb, rpb)]["total"] += s["total"]
        level2[(ob, wb, rpb)]["wins"] += s["wins"]
        level3[(ob, wb)]["total"] += s["total"]
        level3[(ob, wb)]["wins"] += s["wins"]
        level4[(ob,)]["total"] += s["total"]
        level4[(ob,)]["wins"] += s["wins"]
        global_total += s["total"]
        global_wins += s["wins"]

    global_prob = global_wins / global_total if global_total > 0 else 0.5

    def lookup(row):
        ob, wb, rpb, edb = bucketize(row)
        k1 = (ob, wb, rpb, edb)
        if k1 in counts and counts[k1]["total"] >= MIN_SAMPLE:
            s = counts[k1]
            return s["wins"] / s["total"]
        k2 = (ob, wb, rpb)
        if level2[k2]["total"] >= MIN_SAMPLE:
            return level2[k2]["wins"] / level2[k2]["total"]
        k3 = (ob, wb)
        if level3[k3]["total"] >= MIN_SAMPLE:
            return level3[k3]["wins"] / level3[k3]["total"]
        k4 = (ob,)
        if level4[k4]["total"] >= MIN_SAMPLE:
            return level4[k4]["wins"] / level4[k4]["total"]
        return global_prob

    return lookup


def compute_calibration(predictions):
    decile_bins = [{"total": 0, "actual_wins": 0, "sum_pred": 0.0} for _ in range(10)]

    for pred, actual in predictions:
        idx = min(int(pred * 10), 9)
        decile_bins[idx]["total"] += 1
        decile_bins[idx]["actual_wins"] += actual
        decile_bins[idx]["sum_pred"] += pred

    calibration = []
    for b in decile_bins:
        if b["total"] > 0:
            calibration.append(round(b["actual_wins"] / b["total"], 4))
        else:
            calibration.append("")

    return calibration


def build_rolling_backtest():
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    metadata_path = os.path.join(PROCESSED_DIR, "match_metadata.csv")
    with open(metadata_path, "r", encoding="utf-8") as f:
        meta_rows = list(csv.DictReader(f))
    match_dates = {r["match_id"]: r["date"] for r in meta_rows}

    with open(INPUT_PATH, "r", encoding="utf-8") as f:
        all_rows = list(csv.DictReader(f))

    for row in all_rows:
        row["date"] = match_dates.get(row["match_id"], "")

    all_years = sorted(set(get_year(r["date"]) for r in all_rows if r.get("date", "")))

    results = []
    window_num = 0

    for i in range(len(all_years)):
        test_year = all_years[i]
        train_end = test_year - 1
        train_start = train_end - TRAIN_YEARS + 1

        if train_start < all_years[0]:
            continue

        train_rows = [r for r in all_rows if train_start <= get_year(r["date"]) <= train_end]
        test_rows = [r for r in all_rows if get_year(r["date"]) == test_year]

        if not train_rows or not test_rows:
            continue

        window_num += 1
        lookup_fn = build_model_from_rows(train_rows)

        predictions = []
        correct = 0
        brier_sum = 0.0
        log_loss_sum = 0.0

        for row in test_rows:
            pred = lookup_fn(row)
            actual = 1 if row["batting_team"] == row["eventual_winner"] else 0

            predictions.append((pred, actual))
            brier_sum += (pred - actual) ** 2

            eps = 1e-15
            p_clipped = max(eps, min(1 - eps, pred))
            log_loss_sum += -(actual * math.log(p_clipped) + (1 - actual) * math.log(1 - p_clipped))

            predicted_win = 1 if pred >= 0.5 else 0
            if predicted_win == actual:
                correct += 1

        n = len(test_rows)
        brier = round(brier_sum / n, 6)
        ll = round(log_loss_sum / n, 6)
        acc = round(correct / n, 4)
        avg_pred = round(sum(p for p, _ in predictions) / n, 4)

        calibration = compute_calibration(predictions)
        test_matches = len(set(r["match_id"] for r in test_rows))

        result = {
            "window": window_num,
            "train_start": train_start,
            "train_end": train_end,
            "test_year": test_year,
            "train_rows": len(train_rows),
            "test_rows": n,
            "test_matches": test_matches,
            "brier_score": brier,
            "log_loss": ll,
            "accuracy": acc,
            "avg_predicted_prob": avg_pred,
        }
        for d in range(10):
            result[f"calibration_decile_{d+1}"] = calibration[d]

        results.append(result)

    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(results)

    brier_scores = [r["brier_score"] for r in results]
    accuracies = [r["accuracy"] for r in results]

    print("\n=== build_rolling_backtest.py Summary ===")
    print(f"  Total windows evaluated      : {len(results)}")
    print(f"  Train period                 : {TRAIN_YEARS} years")
    print(f"  Output                       : {OUTPUT_PATH}")
    print(f"\n  Average Brier score          : {sum(brier_scores)/len(brier_scores):.6f}")
    print(f"  Best window Brier            : {min(brier_scores):.6f}")
    print(f"  Worst window Brier           : {max(brier_scores):.6f}")
    print(f"  Average accuracy             : {sum(accuracies)/len(accuracies):.4f}")
    print(f"\n  Per-window breakdown:")
    for r in results:
        print(f"    Window {r['window']:2d}: train={r['train_start']}-{r['train_end']} "
              f"test={r['test_year']} "
              f"matches={r['test_matches']:3d} "
              f"brier={r['brier_score']:.6f} "
              f"logloss={r['log_loss']:.6f} "
              f"acc={r['accuracy']:.4f}")

    return results


if __name__ == "__main__":
    build_rolling_backtest()
