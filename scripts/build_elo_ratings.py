import csv
import math
import os
import sys


INPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "processed", "match_metadata.csv")
HISTORY_OUTPUT = os.path.join(os.path.dirname(__file__), "..", "data", "processed", "elo_ratings_history.csv")
CURRENT_OUTPUT = os.path.join(os.path.dirname(__file__), "..", "data", "processed", "current_team_ratings.csv")

K_FACTOR = 20
INITIAL_RATING = 1500

HISTORY_FIELDS = [
    "match_id", "season", "date", "venue",
    "team_1", "team_2",
    "pre_match_rating_team_1", "pre_match_rating_team_2",
    "rating_difference", "expected_win_probability_team_1",
    "expected_win_probability_team_2", "winner",
    "post_match_rating_team_1", "post_match_rating_team_2"
]

CURRENT_FIELDS = ["team", "rating", "matches_played", "wins", "losses"]


def expected_score(rating_a, rating_b):
    return 1.0 / (1.0 + math.pow(10, (rating_b - rating_a) / 400.0))


def update_elo(rating, expected, actual, k=K_FACTOR):
    return round(rating + k * (actual - expected), 2)


def load_matches(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        matches = list(reader)
    matches.sort(key=lambda m: m["date"])
    return matches


def build_elo_ratings():
    os.makedirs(os.path.dirname(HISTORY_OUTPUT), exist_ok=True)
    matches = load_matches(INPUT_PATH)

    ratings = {}
    team_stats = {}

    history_rows = []

    for match in matches:
        team_1 = match["team_1"]
        team_2 = match["team_2"]
        winner = match["winner"]

        if team_1 not in ratings:
            ratings[team_1] = INITIAL_RATING
            team_stats[team_1] = {"matches_played": 0, "wins": 0, "losses": 0}
        if team_2 not in ratings:
            ratings[team_2] = INITIAL_RATING
            team_stats[team_2] = {"matches_played": 0, "wins": 0, "losses": 0}

        r1 = ratings[team_1]
        r2 = ratings[team_2]

        e1 = expected_score(r1, r2)
        e2 = expected_score(r2, r1)

        s1 = 1.0 if winner == team_1 else 0.0
        s2 = 1.0 if winner == team_2 else 0.0

        new_r1 = update_elo(r1, e1, s1)
        new_r2 = update_elo(r2, e2, s2)

        history_rows.append({
            "match_id": match["match_id"],
            "season": match["season"],
            "date": match["date"],
            "venue": match["venue"],
            "team_1": team_1,
            "team_2": team_2,
            "pre_match_rating_team_1": r1,
            "pre_match_rating_team_2": r2,
            "rating_difference": round(r1 - r2, 2),
            "expected_win_probability_team_1": round(e1, 4),
            "expected_win_probability_team_2": round(e2, 4),
            "winner": winner,
            "post_match_rating_team_1": new_r1,
            "post_match_rating_team_2": new_r2,
        })

        ratings[team_1] = new_r1
        ratings[team_2] = new_r2

        team_stats[team_1]["matches_played"] += 1
        team_stats[team_2]["matches_played"] += 1
        if winner == team_1:
            team_stats[team_1]["wins"] += 1
            team_stats[team_2]["losses"] += 1
        else:
            team_stats[team_1]["losses"] += 1
            team_stats[team_2]["wins"] += 1

    with open(HISTORY_OUTPUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HISTORY_FIELDS)
        writer.writeheader()
        writer.writerows(history_rows)

    current_rows = []
    for team in sorted(ratings, key=lambda t: ratings[t], reverse=True):
        current_rows.append({
            "team": team,
            "rating": ratings[team],
            "matches_played": team_stats[team]["matches_played"],
            "wins": team_stats[team]["wins"],
            "losses": team_stats[team]["losses"],
        })

    with open(CURRENT_OUTPUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CURRENT_FIELDS)
        writer.writeheader()
        writer.writerows(current_rows)

    print("\n=== build_elo_ratings.py Summary ===")
    print(f"  Total matches processed    : {len(history_rows)}")
    print(f"  Total teams tracked        : {len(ratings)}")
    print(f"  K-factor                   : {K_FACTOR}")
    print(f"  Initial rating             : {INITIAL_RATING}")
    print(f"  History output             : {HISTORY_OUTPUT}")
    print(f"  Current ratings output     : {CURRENT_OUTPUT}")
    print(f"\n  Top 5 highest rated teams:")
    for i, row in enumerate(current_rows[:5], 1):
        print(f"    {i}. {row['team']:30s} — Rating: {row['rating']:.2f}  (W:{row['wins']} L:{row['losses']})")

    return history_rows, current_rows


if __name__ == "__main__":
    build_elo_ratings()
