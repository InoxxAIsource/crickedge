import csv
import os
import sys


PROCESSED_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "processed")

ELO_HISTORY_PATH = os.path.join(PROCESSED_DIR, "elo_ratings_history.csv")
SNAPSHOTS_PATH = os.path.join(PROCESSED_DIR, "over_state_snapshots.csv")
POWERPLAY_PATH = os.path.join(PROCESSED_DIR, "powerplay_summary.csv")

SNAPSHOTS_OUTPUT = os.path.join(PROCESSED_DIR, "over_state_snapshots_enriched.csv")
POWERPLAY_OUTPUT = os.path.join(PROCESSED_DIR, "powerplay_summary_enriched.csv")


def load_csv(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def build_elo_lookup(elo_rows):
    lookup = {}
    for row in elo_rows:
        mid = row["match_id"]
        lookup[mid] = {
            "team_1": row["team_1"],
            "team_2": row["team_2"],
            "pre_elo_1": float(row["pre_match_rating_team_1"]),
            "pre_elo_2": float(row["pre_match_rating_team_2"]),
            "exp_win_1": float(row["expected_win_probability_team_1"]),
            "exp_win_2": float(row["expected_win_probability_team_2"]),
        }
    return lookup


def get_elo_for_teams(elo_entry, batting_team, bowling_team):
    if elo_entry["team_1"] == batting_team:
        bat_elo = elo_entry["pre_elo_1"]
        bowl_elo = elo_entry["pre_elo_2"]
        bat_exp = elo_entry["exp_win_1"]
    elif elo_entry["team_2"] == batting_team:
        bat_elo = elo_entry["pre_elo_2"]
        bowl_elo = elo_entry["pre_elo_1"]
        bat_exp = elo_entry["exp_win_2"]
    else:
        return None
    return {
        "batting_team_pre_elo": bat_elo,
        "bowling_team_pre_elo": bowl_elo,
        "elo_rating_difference": round(bat_elo - bowl_elo, 2),
        "pre_match_expected_win_prob": round(bat_exp, 4),
    }


def enrich_snapshots(snapshot_rows, elo_lookup):
    enriched = []
    null_count = 0
    for row in snapshot_rows:
        mid = row["match_id"]
        elo_entry = elo_lookup.get(mid)
        if elo_entry:
            elo_data = get_elo_for_teams(elo_entry, row["batting_team"], row["bowling_team"])
        else:
            elo_data = None

        if elo_data:
            row.update(elo_data)
        else:
            null_count += 1
            row["batting_team_pre_elo"] = ""
            row["bowling_team_pre_elo"] = ""
            row["elo_rating_difference"] = ""
            row["pre_match_expected_win_prob"] = ""
        enriched.append(row)
    return enriched, null_count


def enrich_powerplay(pp_rows, elo_lookup):
    enriched = []
    null_count = 0
    for row in pp_rows:
        mid = row["match_id"]
        elo_entry = elo_lookup.get(mid)
        if elo_entry:
            elo_data = get_elo_for_teams(elo_entry, row["batting_team"], row["bowling_team"])
        else:
            elo_data = None

        if elo_data:
            row.update(elo_data)
        else:
            null_count += 1
            row["batting_team_pre_elo"] = ""
            row["bowling_team_pre_elo"] = ""
            row["elo_rating_difference"] = ""
            row["pre_match_expected_win_prob"] = ""
        enriched.append(row)
    return enriched, null_count


def write_csv(filepath, rows, fieldnames):
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def build_enriched_datasets():
    elo_rows = load_csv(ELO_HISTORY_PATH)
    elo_lookup = build_elo_lookup(elo_rows)

    snapshot_rows = load_csv(SNAPSHOTS_PATH)
    enriched_snapshots, snap_nulls = enrich_snapshots(snapshot_rows, elo_lookup)

    snap_fields = list(snapshot_rows[0].keys())
    for col in ["batting_team_pre_elo", "bowling_team_pre_elo", "elo_rating_difference", "pre_match_expected_win_prob"]:
        if col not in snap_fields:
            snap_fields.append(col)
    write_csv(SNAPSHOTS_OUTPUT, enriched_snapshots, snap_fields)

    pp_rows = load_csv(POWERPLAY_PATH)
    enriched_pp, pp_nulls = enrich_powerplay(pp_rows, elo_lookup)

    pp_fields = list(pp_rows[0].keys())
    for col in ["batting_team_pre_elo", "bowling_team_pre_elo", "elo_rating_difference", "pre_match_expected_win_prob"]:
        if col not in pp_fields:
            pp_fields.append(col)
    write_csv(POWERPLAY_OUTPUT, enriched_pp, pp_fields)

    print("\n=== build_enriched_datasets.py Summary ===")
    print(f"\n  Over State Snapshots Enriched:")
    print(f"    Total rows          : {len(enriched_snapshots)}")
    print(f"    Null Elo values     : {snap_nulls}")
    print(f"    Output              : {SNAPSHOTS_OUTPUT}")
    print(f"\n  Sample 5 rows (over_state_snapshots_enriched):")
    for row in enriched_snapshots[:5]:
        print(f"    match={row['match_id']} inn={row['innings_number']} over={row['over_number']} "
              f"bat_elo={row['batting_team_pre_elo']} bowl_elo={row['bowling_team_pre_elo']} "
              f"elo_diff={row['elo_rating_difference']} exp_win={row['pre_match_expected_win_prob']}")

    print(f"\n  Powerplay Summary Enriched:")
    print(f"    Total rows          : {len(enriched_pp)}")
    print(f"    Null Elo values     : {pp_nulls}")
    print(f"    Output              : {POWERPLAY_OUTPUT}")
    print(f"\n  Sample 5 rows (powerplay_summary_enriched):")
    for row in enriched_pp[:5]:
        print(f"    match={row['match_id']} bat={row['batting_team'][:20]:20s} "
              f"bat_elo={row['batting_team_pre_elo']} bowl_elo={row['bowling_team_pre_elo']} "
              f"elo_diff={row['elo_rating_difference']} exp_win={row['pre_match_expected_win_prob']}")

    if snap_nulls == 0 and pp_nulls == 0:
        print(f"\n  CONFIRMED: No null Elo values in either enriched dataset.")
    else:
        print(f"\n  WARNING: Found null Elo values — snapshots: {snap_nulls}, powerplay: {pp_nulls}", file=sys.stderr)

    return enriched_snapshots, enriched_pp


if __name__ == "__main__":
    build_enriched_datasets()
