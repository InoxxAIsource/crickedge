import csv
import os


PROCESSED_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "processed")
METADATA_PATH = os.path.join(PROCESSED_DIR, "match_metadata.csv")
ODDS_PATH = os.path.join(PROCESSED_DIR, "historical_odds_normalized.csv")
OUTPUT_PATH = os.path.join(PROCESSED_DIR, "match_metadata_with_odds.csv")


def merge_odds_metadata():
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    with open(METADATA_PATH, "r", encoding="utf-8") as f:
        metadata = list(csv.DictReader(f))

    with open(ODDS_PATH, "r", encoding="utf-8") as f:
        odds_rows = list(csv.DictReader(f))

    odds_lookup = {r["match_id"]: r for r in odds_rows}

    odds_fields = [
        "bookmaker_name", "team_1_odds", "team_2_odds",
        "team_1_implied_prob", "team_2_implied_prob",
        "market_overround", "team_1_market_prob", "team_2_market_prob"
    ]

    meta_fields = list(metadata[0].keys()) if metadata else []
    output_fields = meta_fields + odds_fields

    output_rows = []
    matched = 0
    unmatched = 0

    for row in metadata:
        mid = row["match_id"]
        odds = odds_lookup.get(mid)

        if odds:
            for field in odds_fields:
                row[field] = odds[field]
            matched += 1
        else:
            for field in odds_fields:
                row[field] = ""
            unmatched += 1

        output_rows.append(row)

    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=output_fields)
        writer.writeheader()
        writer.writerows(output_rows)

    print("\n=== merge_odds_metadata.py Summary ===")
    print(f"  Total matches              : {len(metadata)}")
    print(f"  Matches with odds          : {matched}")
    print(f"  Matches without odds       : {unmatched}")
    print(f"  Output                     : {OUTPUT_PATH}")

    return output_rows


if __name__ == "__main__":
    merge_odds_metadata()
