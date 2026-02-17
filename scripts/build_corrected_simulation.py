import csv
import os


PROCESSED_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "processed")
OLD_INPUT_PATH = os.path.join(PROCESSED_DIR, "high_confidence_inplay_odds.csv")
NEW_INPUT_PATH = os.path.join(PROCESSED_DIR, "per_over_aligned_odds.csv")
OUTPUT_PATH = os.path.join(PROCESSED_DIR, "corrected_edge_simulation_results.csv")

THRESHOLDS = [0.03, 0.05, 0.07, 0.10, 0.15]

COMMISSION_RATE = 0.05
SLIPPAGE_TICKS = 1
TICK_SIZE = 0.02
EXECUTION_DELAY_PENALTY = 0.01

OUTPUT_FIELDS = [
    "data_source", "threshold", "scenario",
    "total_trades", "wins", "losses", "win_rate",
    "total_staked", "profit", "roi_pct",
    "max_drawdown", "max_drawdown_pct",
    "avg_edge", "avg_market_prob",
    "avg_effective_odds", "avg_commission_paid",
    "total_commission", "total_slippage_cost",
]


def apply_slippage(odds, ticks):
    return max(1.01, odds - ticks * TICK_SIZE)


def apply_commission(profit, rate):
    if profit > 0:
        return profit * (1.0 - rate)
    return profit


def run_simulation(rows, threshold, scenario="gross", first_only=True):
    seen_matches = set()
    trades = []
    bankroll_history = [0.0]
    peak = 0.0
    max_drawdown = 0.0
    total_commission = 0.0
    total_slippage = 0.0
    edge_sum = 0.0
    market_prob_sum = 0.0

    for row in rows:
        mid = row["match_id"]
        edge = float(row["edge"])
        model_prob = float(row["model_probability"])
        raw_odds = float(row["market_odds_1"])
        market_prob = float(row["market_prob_1"])
        batting_team = row["batting_team"]
        winner = row["eventual_winner"]

        effective_edge = edge
        if scenario in ("realistic", "worst_case"):
            effective_edge -= EXECUTION_DELAY_PENALTY

        if effective_edge < threshold:
            continue

        if first_only and mid in seen_matches:
            continue
        seen_matches.add(mid)

        won = (batting_team == winner)

        if scenario == "gross":
            effective_odds = raw_odds
            pnl = (effective_odds - 1.0) if won else -1.0
            commission = 0.0
            slippage = 0.0
        elif scenario == "realistic":
            effective_odds = apply_slippage(raw_odds, SLIPPAGE_TICKS)
            gross_pnl = (effective_odds - 1.0) if won else -1.0
            pnl = apply_commission(gross_pnl, COMMISSION_RATE)
            commission = gross_pnl - pnl if gross_pnl > 0 else 0
            slippage = raw_odds - effective_odds
        elif scenario == "worst_case":
            effective_odds = apply_slippage(raw_odds, SLIPPAGE_TICKS * 2)
            gross_pnl = (effective_odds - 1.0) if won else -1.0
            pnl = apply_commission(gross_pnl, COMMISSION_RATE * 1.5)
            commission = gross_pnl - pnl if gross_pnl > 0 else 0
            slippage = raw_odds - effective_odds
        else:
            effective_odds = raw_odds
            pnl = (effective_odds - 1.0) if won else -1.0
            commission = 0.0
            slippage = 0.0

        total_commission += commission
        total_slippage += slippage
        edge_sum += edge
        market_prob_sum += market_prob

        trades.append({
            "match_id": mid,
            "won": won,
            "pnl": pnl,
            "odds": raw_odds,
            "effective_odds": effective_odds,
            "commission": commission,
        })

        cumulative = bankroll_history[-1] + pnl
        bankroll_history.append(cumulative)
        peak = max(peak, cumulative)
        dd = peak - cumulative
        max_drawdown = max(max_drawdown, dd)

    total = len(trades)
    if total == 0:
        return {
            "threshold": threshold, "scenario": scenario,
            "total_trades": 0, "wins": 0, "losses": 0, "win_rate": 0,
            "total_staked": 0, "profit": 0, "roi_pct": 0,
            "max_drawdown": 0, "max_drawdown_pct": 0,
            "avg_edge": 0, "avg_market_prob": 0,
            "avg_effective_odds": 0, "avg_commission_paid": 0,
            "total_commission": 0, "total_slippage_cost": 0,
        }

    wins = sum(1 for t in trades if t["won"])
    losses = total - wins
    total_staked = total
    profit = round(sum(t["pnl"] for t in trades), 4)
    roi = round((profit / total_staked) * 100, 2) if total_staked > 0 else 0
    win_rate = round(wins / total, 4)
    max_dd_pct = round((max_drawdown / total_staked) * 100, 2) if total_staked > 0 else 0
    avg_eff_odds = round(sum(t["effective_odds"] for t in trades) / total, 4)
    avg_comm = round(total_commission / total, 4) if total > 0 else 0
    avg_edge = round(edge_sum / total, 4)
    avg_mkt_prob = round(market_prob_sum / total, 4)

    return {
        "threshold": threshold,
        "scenario": scenario,
        "total_trades": total,
        "wins": wins,
        "losses": losses,
        "win_rate": win_rate,
        "total_staked": total_staked,
        "profit": round(profit, 2),
        "roi_pct": roi,
        "max_drawdown": round(max_drawdown, 2),
        "max_drawdown_pct": max_dd_pct,
        "avg_edge": avg_edge,
        "avg_market_prob": avg_mkt_prob,
        "avg_effective_odds": avg_eff_odds,
        "avg_commission_paid": avg_comm,
        "total_commission": round(total_commission, 2),
        "total_slippage_cost": round(total_slippage, 2),
    }


def main():
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    with open(OLD_INPUT_PATH, "r", encoding="utf-8") as f:
        old_rows = list(csv.DictReader(f))
    old_rows.sort(key=lambda r: (r["date"], r["match_id"],
                                  int(r["innings_number"]), int(r["over_number"])))

    with open(NEW_INPUT_PATH, "r", encoding="utf-8") as f:
        new_rows = list(csv.DictReader(f))
    new_rows.sort(key=lambda r: (r["date"], r["match_id"],
                                  int(r["innings_number"]), int(r["over_number"])))

    print(f"\n=== build_corrected_simulation.py ===")
    print(f"  Old data (single-TS): {len(old_rows)} rows, "
          f"{len(set(r['match_id'] for r in old_rows))} matches")
    print(f"  New data (per-over) : {len(new_rows)} rows, "
          f"{len(set(r['match_id'] for r in new_rows))} matches")

    scenarios = ["gross", "realistic", "worst_case"]
    results = []

    for source_name, source_rows in [("single_timestamp", old_rows), ("per_over_aligned", new_rows)]:
        for scenario in scenarios:
            for threshold in THRESHOLDS:
                result = run_simulation(source_rows, threshold, scenario)
                result["data_source"] = source_name
                results.append(result)

    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(results)

    for source_name in ["single_timestamp", "per_over_aligned"]:
        print(f"\n  === {source_name.upper().replace('_', ' ')} ===")
        for scenario in scenarios:
            print(f"\n  --- {scenario.upper()} ---")
            print(f"  {'Threshold':>10s} {'Trades':>7s} {'Wins':>6s} {'Win%':>7s} "
                  f"{'Profit':>8s} {'ROI%':>7s} {'AvgEdge':>8s} {'MaxDD':>7s}")
            print(f"  {'-'*10} {'-'*7} {'-'*6} {'-'*7} {'-'*8} {'-'*7} {'-'*8} {'-'*7}")
            for r in results:
                if r["data_source"] != source_name or r["scenario"] != scenario:
                    continue
                print(f"  {r['threshold']:>10.0%} {r['total_trades']:>7d} {r['wins']:>6d} "
                      f"{r['win_rate']:>7.1%} {r['profit']:>8.2f} {r['roi_pct']:>6.1f}% "
                      f"{r['avg_edge']:>7.1%} {r['max_drawdown']:>7.2f}")

    print(f"\n  Output: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
