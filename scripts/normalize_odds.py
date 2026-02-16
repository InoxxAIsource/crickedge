import csv
import os


PROCESSED_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "processed")
INPUT_PATH = os.path.join(PROCESSED_DIR, "historical_odds_raw.csv")
OUTPUT_PATH = os.path.join(PROCESSED_DIR, "historical_odds_normalized.csv")

OUTPUT_FIELDS = [
    "match_id", "date", "team_1", "team_2",
    "bookmaker_name", "team_1_odds", "team_2_odds",
    "team_1_implied_prob", "team_2_implied_prob",
    "market_overround",
    "team_1_market_prob", "team_2_market_prob",
    "snapshot_timestamp"
]


def normalize_odds():
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    with open(INPUT_PATH, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    output_rows = []

    for row in rows:
        t1_odds = float(row["team_1_odds"])
        t2_odds = float(row["team_2_odds"])

        t1_implied = 1.0 / t1_odds
        t2_implied = 1.0 / t2_odds

        overround = t1_implied + t2_implied

        t1_market_prob = t1_implied / overround
        t2_market_prob = t2_implied / overround

        output_rows.append({
            "match_id": row["match_id"],
            "date": row["date"],
            "team_1": row["team_1"],
            "team_2": row["team_2"],
            "bookmaker_name": row["bookmaker_name"],
            "team_1_odds": t1_odds,
            "team_2_odds": t2_odds,
            "team_1_implied_prob": round(t1_implied, 6),
            "team_2_implied_prob": round(t2_implied, 6),
            "market_overround": round(overround, 6),
            "team_1_market_prob": round(t1_market_prob, 6),
            "team_2_market_prob": round(t2_market_prob, 6),
            "snapshot_timestamp": row["snapshot_timestamp"],
        })

    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(output_rows)

    print("\n=== normalize_odds.py Summary ===")
    print(f"  Total rows processed       : {len(output_rows)}")
    if output_rows:
        avg_overround = sum(float(r["market_overround"]) for r in output_rows) / len(output_rows)
        print(f"  Average market overround   : {avg_overround:.4f}")
    print(f"  Output                     : {OUTPUT_PATH}")

    return output_rows


if __name__ == "__main__":
    normalize_odds()
