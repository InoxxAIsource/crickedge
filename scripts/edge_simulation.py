import csv
import os


PROCESSED_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "processed")
METADATA_ODDS_PATH = os.path.join(PROCESSED_DIR, "match_metadata_with_odds.csv")
ELO_HISTORY_PATH = os.path.join(PROCESSED_DIR, "elo_ratings_history.csv")
OUTPUT_PATH = os.path.join(PROCESSED_DIR, "edge_simulation_results.csv")

THRESHOLDS = [0.03, 0.05, 0.07, 0.10]

OUTPUT_FIELDS = [
    "threshold", "total_bets", "wins", "losses",
    "win_rate", "total_staked", "total_return",
    "profit", "roi_pct", "max_drawdown", "max_drawdown_pct"
]


def load_model_probs():
    elo_path = ELO_HISTORY_PATH
    with open(elo_path, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    lookup = {}
    for r in rows:
        lookup[r["match_id"]] = {
            "team_1": r["team_1"],
            "team_2": r["team_2"],
            "exp_win_1": float(r["expected_win_probability_team_1"]),
            "exp_win_2": float(r["expected_win_probability_team_2"]),
        }
    return lookup


def run_simulation(matches, model_probs, threshold):
    bets = []
    bankroll_history = [0.0]
    peak = 0.0
    max_drawdown = 0.0

    for match in matches:
        mid = match["match_id"]
        if match["team_1_market_prob"] == "" or mid not in model_probs:
            continue

        t1_market = float(match["team_1_market_prob"])
        t2_market = float(match["team_2_market_prob"])
        t1_odds = float(match["team_1_odds"])
        t2_odds = float(match["team_2_odds"])
        winner = match["winner"]
        if not winner or winner not in (match["team_1"], match["team_2"]):
            continue

        mp = model_probs[mid]

        t1_edge = mp["exp_win_1"] - t1_market
        t2_edge = mp["exp_win_2"] - t2_market

        bet_team = None
        bet_odds = None
        if t1_edge >= threshold and t1_edge >= t2_edge:
            bet_team = mp["team_1"]
            bet_odds = t1_odds
        elif t2_edge >= threshold:
            bet_team = mp["team_2"]
            bet_odds = t2_odds

        if bet_team is None:
            continue

        won = (bet_team == winner)
        pnl = (bet_odds - 1.0) if won else -1.0
        bets.append({
            "match_id": mid,
            "bet_team": bet_team,
            "odds": bet_odds,
            "won": won,
            "pnl": pnl,
        })

        cumulative = bankroll_history[-1] + pnl
        bankroll_history.append(cumulative)
        peak = max(peak, cumulative)
        drawdown = peak - cumulative
        max_drawdown = max(max_drawdown, drawdown)

    total_bets = len(bets)
    if total_bets == 0:
        return {
            "threshold": threshold,
            "total_bets": 0, "wins": 0, "losses": 0,
            "win_rate": 0, "total_staked": 0, "total_return": 0,
            "profit": 0, "roi_pct": 0,
            "max_drawdown": 0, "max_drawdown_pct": 0,
        }

    wins = sum(1 for b in bets if b["won"])
    losses = total_bets - wins
    total_staked = total_bets
    total_return = round(sum(b["pnl"] for b in bets) + total_staked, 2)
    profit = round(total_return - total_staked, 2)
    roi = round((profit / total_staked) * 100, 2) if total_staked > 0 else 0
    win_rate = round(wins / total_bets, 4)
    max_dd_pct = round((max_drawdown / total_staked) * 100, 2) if total_staked > 0 else 0

    return {
        "threshold": threshold,
        "total_bets": total_bets,
        "wins": wins,
        "losses": losses,
        "win_rate": win_rate,
        "total_staked": total_staked,
        "total_return": total_return,
        "profit": profit,
        "roi_pct": roi,
        "max_drawdown": round(max_drawdown, 2),
        "max_drawdown_pct": max_dd_pct,
    }


def edge_simulation():
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    with open(METADATA_ODDS_PATH, "r", encoding="utf-8") as f:
        all_matches = list(csv.DictReader(f))

    matches_with_odds = [m for m in all_matches if m.get("team_1_market_prob", "") != ""]
    matches_with_odds.sort(key=lambda m: m["date"])

    model_probs = load_model_probs()

    print("\n=== edge_simulation.py Summary ===")
    print(f"  Total matches with odds    : {len(matches_with_odds)}")

    results = []
    for threshold in THRESHOLDS:
        result = run_simulation(matches_with_odds, model_probs, threshold)
        results.append(result)

    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(results)

    print(f"\n  {'Threshold':>10s} {'Bets':>6s} {'Wins':>6s} {'Win%':>7s} "
          f"{'Profit':>8s} {'ROI%':>7s} {'MaxDD':>7s}")
    print(f"  {'-'*10} {'-'*6} {'-'*6} {'-'*7} {'-'*8} {'-'*7} {'-'*7}")
    for r in results:
        print(f"  {r['threshold']:>10.0%} {r['total_bets']:>6d} {r['wins']:>6d} "
              f"{r['win_rate']:>7.1%} {r['profit']:>8.2f} {r['roi_pct']:>6.1f}% "
              f"{r['max_drawdown']:>7.2f}")

    print(f"\n  Output: {OUTPUT_PATH}")

    return results


if __name__ == "__main__":
    edge_simulation()
