import csv
import os


PROCESSED_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "processed")
INPUT_PATH = os.path.join(PROCESSED_DIR, "high_confidence_inplay_odds.csv")
OUTPUT_PATH = os.path.join(PROCESSED_DIR, "live_edge_simulation_results.csv")

THRESHOLDS = [0.05, 0.07, 0.10, 0.15]

OUTPUT_FIELDS = [
    "threshold", "total_trades", "wins", "losses",
    "win_rate", "total_staked", "total_return",
    "profit", "roi_pct", "max_drawdown", "max_drawdown_pct"
]


def run_simulation(rows, threshold):
    seen_matches = set()
    trades = []
    bankroll_history = [0.0]
    peak = 0.0
    max_drawdown = 0.0

    for row in rows:
        mid = row["match_id"]
        edge = float(row["edge"])
        model_prob = float(row["model_probability"])
        market_odds = float(row["market_odds_1"])
        batting_team = row["batting_team"]
        winner = row["eventual_winner"]

        if edge < threshold:
            continue

        if mid in seen_matches:
            continue
        seen_matches.add(mid)

        won = (batting_team == winner)
        pnl = (market_odds - 1.0) if won else -1.0

        trades.append({
            "match_id": mid,
            "batting_team": batting_team,
            "over": row["over_number"],
            "innings": row["innings_number"],
            "model_prob": model_prob,
            "market_prob": float(row["market_prob_1"]),
            "edge": edge,
            "odds": market_odds,
            "won": won,
            "pnl": pnl,
        })

        cumulative = bankroll_history[-1] + pnl
        bankroll_history.append(cumulative)
        peak = max(peak, cumulative)
        dd = peak - cumulative
        max_drawdown = max(max_drawdown, dd)

    total = len(trades)
    if total == 0:
        return {
            "threshold": threshold,
            "total_trades": 0, "wins": 0, "losses": 0,
            "win_rate": 0, "total_staked": 0, "total_return": 0,
            "profit": 0, "roi_pct": 0,
            "max_drawdown": 0, "max_drawdown_pct": 0,
        }

    wins = sum(1 for t in trades if t["won"])
    losses = total - wins
    total_staked = total
    profit = round(sum(t["pnl"] for t in trades), 2)
    total_return = round(profit + total_staked, 2)
    roi = round((profit / total_staked) * 100, 2) if total_staked > 0 else 0
    win_rate = round(wins / total, 4)
    max_dd_pct = round((max_drawdown / total_staked) * 100, 2) if total_staked > 0 else 0

    return {
        "threshold": threshold,
        "total_trades": total,
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


def simulate_live_edge():
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    with open(INPUT_PATH, "r", encoding="utf-8") as f:
        all_rows = list(csv.DictReader(f))

    all_rows.sort(key=lambda r: (r["date"], r["match_id"], int(r["innings_number"]), int(r["over_number"])))

    print(f"\n=== simulate_live_edge_85_plus.py ===")
    print(f"  Total in-play odds rows    : {len(all_rows)}")
    print(f"  Unique matches             : {len(set(r['match_id'] for r in all_rows))}")

    avg_edge = sum(float(r["edge"]) for r in all_rows) / len(all_rows) if all_rows else 0
    print(f"  Average edge               : {avg_edge:.4f}")

    results = []
    for threshold in THRESHOLDS:
        result = run_simulation(all_rows, threshold)
        results.append(result)

    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(results)

    print(f"\n  {'Threshold':>10s} {'Trades':>7s} {'Wins':>6s} {'Win%':>7s} "
          f"{'Profit':>8s} {'ROI%':>7s} {'MaxDD':>7s}")
    print(f"  {'-'*10} {'-'*7} {'-'*6} {'-'*7} {'-'*8} {'-'*7} {'-'*7}")
    for r in results:
        print(f"  {r['threshold']:>10.0%} {r['total_trades']:>7d} {r['wins']:>6d} "
              f"{r['win_rate']:>7.1%} {r['profit']:>8.2f} {r['roi_pct']:>6.1f}% "
              f"{r['max_drawdown']:>7.2f}")

    print(f"\n  Output: {OUTPUT_PATH}")


if __name__ == "__main__":
    simulate_live_edge()
