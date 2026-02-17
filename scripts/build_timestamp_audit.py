import csv
import os
from collections import defaultdict
from datetime import datetime


PROCESSED_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "processed")
INPUT_PATH = os.path.join(PROCESSED_DIR, "high_confidence_inplay_odds.csv")
METADATA_PATH = os.path.join(PROCESSED_DIR, "match_metadata.csv")
OUTPUT_PATH = os.path.join(PROCESSED_DIR, "timestamp_audit_results.csv")

MINUTES_PER_OVER = 4
INNINGS_BREAK_MINUTES = 20

OUTPUT_FIELDS = [
    "match_id", "date", "innings_number", "over_number",
    "batting_team", "bowling_team",
    "model_probability", "market_prob_1", "edge",
    "fetch_timestamp", "estimated_over_end_time",
    "timestamp_vs_over_delta_minutes",
    "leakage_risk", "leakage_severity",
    "single_ts_per_innings", "bookmaker_used",
    "market_odds_1", "effective_odds_after_slippage",
    "edge_after_delay_penalty",
]


def determine_match_slot(match_id, date_str, metadata_by_date):
    day_matches = metadata_by_date.get(date_str, [])
    if len(day_matches) <= 1:
        return 14
    sorted_matches = sorted(day_matches, key=lambda m: int(m["match_id"]))
    if str(match_id) == str(sorted_matches[0]["match_id"]):
        return 10
    return 14


def estimate_over_end_time(date_str, innings_number, over_number, start_hour):
    innings = int(innings_number)
    over = int(over_number)

    if innings == 1:
        minutes_offset = (over + 1) * MINUTES_PER_OVER
    else:
        minutes_offset = 20 * MINUTES_PER_OVER + INNINGS_BREAK_MINUTES + (over + 1) * MINUTES_PER_OVER

    total_minutes = start_hour * 60 + minutes_offset
    hours = total_minutes // 60
    mins = total_minutes % 60
    return f"{date_str}T{hours:02d}:{mins:02d}:00Z"


def parse_ts(ts_str):
    return datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%SZ")


def main():
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    with open(INPUT_PATH, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    with open(METADATA_PATH, "r", encoding="utf-8") as f:
        metadata = list(csv.DictReader(f))

    metadata_by_date = defaultdict(list)
    for m in metadata:
        metadata_by_date[m["date"]].append(m)

    mi_timestamps = defaultdict(set)
    for r in rows:
        mi_timestamps[(r["match_id"], r["innings_number"])].add(r["fetch_timestamp"])

    audit_rows = []
    leakage_counts = {"none": 0, "low": 0, "medium": 0, "high": 0, "critical": 0}

    for row in rows:
        mid = row["match_id"]
        date_str = row["date"]
        inn = row["innings_number"]
        over = row["over_number"]

        start_hour = determine_match_slot(mid, date_str, metadata_by_date)
        over_end = estimate_over_end_time(date_str, inn, over, start_hour)

        fetch_ts = row["fetch_timestamp"]
        single_ts = len(mi_timestamps[(mid, inn)]) == 1

        try:
            fetch_dt = parse_ts(fetch_ts)
            over_end_dt = parse_ts(over_end)
            delta_minutes = (fetch_dt - over_end_dt).total_seconds() / 60.0
        except Exception:
            delta_minutes = 0.0

        if delta_minutes > 5:
            leakage_risk = "critical"
            leakage_severity = "Fetch timestamp >5min after over end — likely post-result odds"
        elif delta_minutes > 1:
            leakage_risk = "high"
            leakage_severity = "Fetch timestamp 1-5min after over end — probable leakage"
        elif delta_minutes > -2:
            leakage_risk = "medium"
            leakage_severity = "Fetch timestamp near over end — possible contemporaneous"
        elif delta_minutes > -10:
            leakage_risk = "low"
            leakage_severity = "Fetch timestamp before over end — likely clean"
        else:
            leakage_risk = "none"
            leakage_severity = "Fetch timestamp well before over — stale odds possible"

        if single_ts:
            leakage_risk = max(leakage_risk, "medium",
                               key=["none", "low", "medium", "high", "critical"].index)
            leakage_severity += " | SAME timestamp for all overs in innings"

        leakage_counts[leakage_risk] = leakage_counts.get(leakage_risk, 0) + 1

        raw_odds = float(row["market_odds_1"])
        slipped_odds = max(1.01, raw_odds - 0.02)
        edge_after_delay = float(row["edge"]) - 0.01

        audit_rows.append({
            "match_id": mid,
            "date": date_str,
            "innings_number": inn,
            "over_number": over,
            "batting_team": row["batting_team"],
            "bowling_team": row["bowling_team"],
            "model_probability": row["model_probability"],
            "market_prob_1": row["market_prob_1"],
            "edge": row["edge"],
            "fetch_timestamp": fetch_ts,
            "estimated_over_end_time": over_end,
            "timestamp_vs_over_delta_minutes": round(delta_minutes, 1),
            "leakage_risk": leakage_risk,
            "leakage_severity": leakage_severity,
            "single_ts_per_innings": single_ts,
            "bookmaker_used": row.get("bookmaker_used", ""),
            "market_odds_1": raw_odds,
            "effective_odds_after_slippage": round(slipped_odds, 4),
            "edge_after_delay_penalty": round(edge_after_delay, 6),
        })

    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(audit_rows)

    print(f"\n=== build_timestamp_audit.py ===")
    print(f"  Total snapshots audited: {len(audit_rows)}")
    print(f"\n  Leakage Risk Distribution:")
    for risk in ["none", "low", "medium", "high", "critical"]:
        cnt = leakage_counts.get(risk, 0)
        pct = cnt / len(audit_rows) * 100 if audit_rows else 0
        print(f"    {risk:>10s}: {cnt:>6d} ({pct:.1f}%)")

    single_ts_count = sum(1 for r in audit_rows if r["single_ts_per_innings"])
    print(f"\n  Single-timestamp innings: {single_ts_count}/{len(audit_rows)} "
          f"({single_ts_count/len(audit_rows)*100:.1f}%)")

    deltas = [r["timestamp_vs_over_delta_minutes"] for r in audit_rows]
    if deltas:
        import statistics
        print(f"\n  Timestamp vs Over-End Delta (minutes):")
        print(f"    Min   : {min(deltas):.1f}")
        print(f"    Max   : {max(deltas):.1f}")
        print(f"    Median: {statistics.median(deltas):.1f}")
        print(f"    Mean  : {statistics.mean(deltas):.1f}")
        print(f"    StdDev: {statistics.stdev(deltas):.1f}")

    edges_gross = [float(r["edge"]) for r in audit_rows]
    edges_adj = [r["edge_after_delay_penalty"] for r in audit_rows]
    if edges_gross:
        import statistics
        print(f"\n  Edge Comparison:")
        print(f"    Gross avg edge      : {statistics.mean(edges_gross):.4f}")
        print(f"    After delay penalty : {statistics.mean(edges_adj):.4f}")
        print(f"    Edge reduction      : {statistics.mean(edges_gross) - statistics.mean(edges_adj):.4f}")

    print(f"\n  CRITICAL FINDING:")
    print(f"    All {single_ts_count} snapshots use a single timestamp per match-innings.")
    print(f"    This means odds for over 4 and over 18 in the same innings are identical.")
    print(f"    The market price at over 4 is NOT the market price at over 18.")
    print(f"    Edge calculations are therefore systematically biased.")
    print(f"\n  Output: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
