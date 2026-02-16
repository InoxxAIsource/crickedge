import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "processed")

st.set_page_config(
    page_title="Crickedge – IPL Analytics",
    page_icon="🏏",
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_data
def load_csv(name):
    return pd.read_csv(os.path.join(DATA_DIR, name))


def load_all():
    data = {}
    data["ratings"] = load_csv("current_team_ratings.csv")
    data["elo_history"] = load_csv("elo_ratings_history.csv")
    data["calibration"] = load_csv("live_model_calibration_2020_plus.csv")
    data["backtest"] = load_csv("rolling_backtest_results.csv")
    data["edge_pre"] = load_csv("edge_simulation_results.csv")
    data["edge_live"] = load_csv("live_edge_simulation_results.csv")
    data["inplay"] = load_csv("high_confidence_inplay_odds.csv")
    data["odds_matches"] = load_csv("match_metadata_with_odds.csv")
    data["bucket_model"] = load_csv("statistical_bucket_model_stabilized.csv")
    return data


data = load_all()

st.sidebar.title("Crickedge")
st.sidebar.caption("IPL Cricket Analytics & Edge Detection")
page = st.sidebar.radio(
    "Navigate",
    ["Overview", "Elo Ratings", "Model Calibration", "Pre-Match Edge", "In-Play Edge", "Bucket Model"],
    index=0,
)

BRAND_COLORS = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
                "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf"]


def page_overview():
    st.title("Crickedge – IPL Analytics Dashboard")
    st.markdown("Cricket analytics platform combining Elo ratings, statistical bucket models, "
                "bookmaker odds analysis, and in-play edge detection for IPL matches (2008–2025).")

    ratings = data["ratings"]
    elo_hist = data["elo_history"]
    edge_pre = data["edge_pre"]
    edge_live = data["edge_live"]
    cal = data["calibration"]

    c1, c2, c3, c4 = st.columns(4)
    total_matches = len(elo_hist)
    seasons = elo_hist["season"].nunique()
    teams = len(ratings)
    brier = cal["abs_calibration_error"].mean()

    c1.metric("Total Matches", f"{total_matches:,}")
    c2.metric("Seasons", seasons)
    c3.metric("Teams Tracked", teams)
    c4.metric("Avg Calibration Error", f"{brier:.3f}")

    st.divider()
    col_l, col_r = st.columns(2)

    with col_l:
        st.subheader("Pre-Match Edge Simulation")
        pre_df = edge_pre.copy()
        pre_df["threshold"] = pre_df["threshold"].apply(lambda x: f"{float(x):.0%}")
        st.dataframe(
            pre_df[["threshold", "total_bets", "wins", "win_rate", "roi_pct", "max_drawdown_pct"]].rename(
                columns={"threshold": "Threshold", "total_bets": "Bets", "wins": "Wins",
                          "win_rate": "Win Rate", "roi_pct": "ROI %", "max_drawdown_pct": "Max DD %"}
            ),
            use_container_width=True, hide_index=True,
        )

    with col_r:
        st.subheader("In-Play Edge Simulation (85%+ confidence)")
        live_df = edge_live.copy()
        live_df["threshold"] = live_df["threshold"].apply(lambda x: f"{float(x):.0%}")
        st.dataframe(
            live_df[["threshold", "total_trades", "wins", "win_rate", "roi_pct", "max_drawdown_pct"]].rename(
                columns={"threshold": "Threshold", "total_trades": "Trades", "wins": "Wins",
                          "win_rate": "Win Rate", "roi_pct": "ROI %", "max_drawdown_pct": "Max DD %"}
            ),
            use_container_width=True, hide_index=True,
        )

    st.divider()
    st.subheader("Current Team Elo Ratings")
    ratings_sorted = ratings.sort_values("rating", ascending=True)
    fig = px.bar(
        ratings_sorted, x="rating", y="team", orientation="h",
        color="rating", color_continuous_scale="RdYlGn",
        labels={"rating": "Elo Rating", "team": ""},
    )
    fig.update_layout(height=450, showlegend=False, coloraxis_showscale=False,
                      margin=dict(l=0, r=20, t=10, b=10))
    fig.update_traces(texttemplate="%{x:.0f}", textposition="outside")
    st.plotly_chart(fig, use_container_width=True)


def page_elo():
    st.title("Elo Rating System")
    st.markdown("Dynamic Elo ratings for all IPL franchises since 2008, with K-factor decay and "
                "season-level tracking. Ratings start at 1500 and adjust after each match.")

    elo = data["elo_history"]
    ratings = data["ratings"]

    tab1, tab2 = st.tabs(["Rating Trajectories", "Current Standings"])

    with tab1:
        teams = sorted(elo["team_1"].unique())
        default_teams = ratings.nlargest(5, "rating")["team"].tolist()
        selected = st.multiselect("Select teams", teams, default=default_teams)

        if selected:
            traces = []
            for team in selected:
                team_matches = elo[(elo["team_1"] == team) | (elo["team_2"] == team)].copy()
                team_matches["team_rating"] = team_matches.apply(
                    lambda r: r["post_match_rating_team_1"] if r["team_1"] == team
                    else r["post_match_rating_team_2"], axis=1
                )
                team_matches = team_matches.sort_values("date")
                traces.append(go.Scatter(
                    x=team_matches["date"], y=team_matches["team_rating"],
                    mode="lines", name=team, line=dict(width=2),
                ))

            fig = go.Figure(data=traces)
            fig.update_layout(
                height=500, xaxis_title="Date", yaxis_title="Elo Rating",
                hovermode="x unified", legend=dict(orientation="h", y=-0.15),
                margin=dict(l=0, r=0, t=10, b=0),
            )
            fig.add_hline(y=1500, line_dash="dot", line_color="gray", annotation_text="Baseline 1500")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Select at least one team to view rating trajectories.")

    with tab2:
        r = ratings.sort_values("rating", ascending=False).reset_index(drop=True)
        r.index += 1
        r.columns = ["Team", "Rating", "Matches", "Wins", "Losses"]
        r["Win %"] = (r["Wins"] / r["Matches"] * 100).round(1)
        r["Rating"] = r["Rating"].round(0).astype(int)
        st.dataframe(r, use_container_width=True)


def page_calibration():
    st.title("Model Calibration Analysis")
    st.markdown("How well the statistical bucket model's predicted probabilities match actual outcomes. "
                "A perfectly calibrated model falls on the diagonal line.")

    cal = data["calibration"]
    backtest = data["backtest"]

    tab1, tab2 = st.tabs(["Calibration Plot", "Rolling Backtest"])

    with tab1:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=[0, 1], y=[0, 1], mode="lines", name="Perfect Calibration",
            line=dict(dash="dash", color="gray"),
        ))
        fig.add_trace(go.Scatter(
            x=cal["avg_predicted_prob"], y=cal["actual_win_rate"],
            mode="markers+lines", name="Model",
            marker=dict(size=cal["sample_count"] / cal["sample_count"].max() * 30 + 5, color="#1f77b4"),
            text=cal.apply(lambda r: f"Decile {r['decile']}<br>"
                                     f"Pred: {r['avg_predicted_prob']:.3f}<br>"
                                     f"Actual: {r['actual_win_rate']:.3f}<br>"
                                     f"n={int(r['sample_count'])}", axis=1),
            hoverinfo="text",
        ))
        fig.update_layout(
            height=500, xaxis_title="Predicted Probability",
            yaxis_title="Actual Win Rate",
            xaxis=dict(range=[0, 1]), yaxis=dict(range=[0, 1]),
            margin=dict(l=0, r=0, t=10, b=0),
        )
        st.plotly_chart(fig, use_container_width=True)

        c1, c2, c3 = st.columns(3)
        c1.metric("Mean Calibration Error", f"{cal['abs_calibration_error'].mean():.4f}")
        c2.metric("Max Calibration Error", f"{cal['abs_calibration_error'].max():.4f}")
        c3.metric("Total Samples", f"{cal['sample_count'].sum():,}")

    with tab2:
        st.subheader("Rolling Window Backtest Results")
        bt = backtest.copy()
        bt["test_year"] = bt["test_year"].astype(str)

        fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                            subplot_titles=["Brier Score by Test Year", "Accuracy by Test Year"],
                            vertical_spacing=0.12)
        fig.add_trace(go.Bar(x=bt["test_year"], y=bt["brier_score"], name="Brier Score",
                             marker_color="#1f77b4"), row=1, col=1)
        fig.add_trace(go.Bar(x=bt["test_year"], y=bt["accuracy"], name="Accuracy",
                             marker_color="#2ca02c"), row=2, col=1)
        fig.update_layout(height=500, showlegend=False, margin=dict(l=0, r=0, t=30, b=0))
        st.plotly_chart(fig, use_container_width=True)

        st.dataframe(
            bt[["window", "test_year", "test_matches", "brier_score", "log_loss", "accuracy"]].rename(
                columns={"window": "Window", "test_year": "Year", "test_matches": "Matches",
                          "brier_score": "Brier", "log_loss": "Log Loss", "accuracy": "Accuracy"}
            ),
            use_container_width=True, hide_index=True,
        )


def page_pre_match_edge():
    st.title("Pre-Match Edge Analysis")
    st.markdown("Compares model predictions against Pinnacle closing odds to identify pre-match "
                "value opportunities. Edge = Model Probability - Market Probability.")

    odds = data["odds_matches"]
    elo = data["elo_history"]
    edge_sim = data["edge_pre"]

    merged = odds.merge(
        elo[["match_id", "expected_win_probability_team_1"]],
        on="match_id", how="left"
    )
    has_odds = merged.dropna(subset=["team_1_market_prob", "expected_win_probability_team_1"]).copy()
    if len(has_odds) == 0:
        st.warning("No matches with bookmaker odds data available.")
        return

    has_odds["model_prob_1"] = has_odds["expected_win_probability_team_1"].astype(float)
    has_odds["market_prob_1"] = has_odds["team_1_market_prob"].astype(float)
    has_odds["edge_1"] = has_odds["model_prob_1"] - has_odds["market_prob_1"]

    tab1, tab2, tab3 = st.tabs(["Edge Distribution", "Simulation Results", "Match Details"])

    with tab1:
        fig = px.histogram(
            has_odds, x="edge_1", nbins=50,
            labels={"edge_1": "Edge (Model - Market)"},
            color_discrete_sequence=["#1f77b4"],
        )
        fig.add_vline(x=0, line_dash="dash", line_color="red")
        fig.update_layout(height=400, margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig, use_container_width=True)

        c1, c2, c3 = st.columns(3)
        c1.metric("Matches with Odds", len(has_odds))
        c2.metric("Avg Edge", f"{has_odds['edge_1'].mean():.4f}")
        c3.metric("Positive Edge %", f"{(has_odds['edge_1'] > 0).mean():.1%}")

    with tab2:
        st.subheader("Flat-Stake Simulation by Edge Threshold")
        sim = edge_sim.copy()

        fig = make_subplots(rows=1, cols=2, subplot_titles=["ROI % by Threshold", "Win Rate by Threshold"])
        fig.add_trace(go.Bar(x=sim["threshold"].apply(lambda x: f"{float(x):.0%}"),
                             y=sim["roi_pct"], name="ROI %",
                             marker_color=sim["roi_pct"].apply(
                                 lambda x: "#2ca02c" if x > 0 else "#d62728")),
                      row=1, col=1)
        fig.add_trace(go.Bar(x=sim["threshold"].apply(lambda x: f"{float(x):.0%}"),
                             y=sim["win_rate"].apply(lambda x: float(x) * 100),
                             name="Win Rate %", marker_color="#1f77b4"),
                      row=1, col=2)
        fig.update_layout(height=400, showlegend=False, margin=dict(l=0, r=0, t=30, b=0))
        st.plotly_chart(fig, use_container_width=True)

        st.dataframe(
            sim.rename(columns={
                "threshold": "Threshold", "total_bets": "Bets", "wins": "Wins",
                "losses": "Losses", "win_rate": "Win Rate", "profit": "Profit",
                "roi_pct": "ROI %", "max_drawdown_pct": "Max DD %",
            })[["Threshold", "Bets", "Wins", "Losses", "Win Rate", "Profit", "ROI %", "Max DD %"]],
            use_container_width=True, hide_index=True,
        )

    with tab3:
        st.subheader("Recent Matches with Edge")
        min_edge = st.slider("Minimum edge filter", -0.3, 0.3, 0.0, 0.01)
        filtered = has_odds[has_odds["edge_1"] >= min_edge].sort_values("date", ascending=False)

        display_cols = {
            "date": "Date", "team_1": "Team 1", "team_2": "Team 2",
            "model_prob_1": "Model %", "market_prob_1": "Market %",
            "edge_1": "Edge", "winner": "Winner",
        }
        show = filtered[list(display_cols.keys())].rename(columns=display_cols).head(50)
        show["Model %"] = show["Model %"].apply(lambda x: f"{x:.1%}")
        show["Market %"] = show["Market %"].apply(lambda x: f"{x:.1%}")
        show["Edge"] = show["Edge"].apply(lambda x: f"{x:+.1%}")
        st.dataframe(show, use_container_width=True, hide_index=True)


def page_inplay_edge():
    st.title("In-Play Edge Analysis")
    st.markdown("In-play Pinnacle odds vs model predictions for high-confidence (85%+) snapshots. "
                "These represent game states where the model is highly confident in the batting team winning.")

    inplay = data["inplay"]
    edge_live = data["edge_live"]

    tab1, tab2, tab3 = st.tabs(["Edge Distribution", "Simulation", "Trade Log"])

    with tab1:
        fig = px.histogram(
            inplay, x="edge", nbins=40,
            labels={"edge": "Edge (Model - Market)"},
            color_discrete_sequence=["#2ca02c"],
        )
        fig.update_layout(height=400, margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig, use_container_width=True)

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("In-Play Snapshots", len(inplay))
        c2.metric("Unique Matches", inplay["match_id"].nunique())
        c3.metric("Avg Edge", f"{inplay['edge'].astype(float).mean():.1%}")
        c4.metric("Win Rate", f"{(inplay['batting_team'] == inplay['eventual_winner']).mean():.1%}")

        st.subheader("Edge by Innings Phase")
        ip = inplay.copy()
        ip["over"] = ip["over_number"].astype(int)
        ip["phase"] = ip["over"].apply(
            lambda o: "Powerplay (1-6)" if o <= 6 else ("Middle (7-15)" if o <= 15 else "Death (16-20)")
        )
        phase_stats = ip.groupby("phase").agg(
            count=("edge", "size"),
            avg_edge=("edge", lambda x: x.astype(float).mean()),
            win_rate=("eventual_winner", lambda x: (ip.loc[x.index, "batting_team"] == x).mean()),
        ).reset_index()

        fig2 = px.bar(phase_stats, x="phase", y="avg_edge",
                      color="avg_edge", color_continuous_scale="RdYlGn",
                      text=phase_stats["avg_edge"].apply(lambda x: f"{x:.1%}"),
                      labels={"phase": "Phase", "avg_edge": "Avg Edge"})
        fig2.update_layout(height=350, showlegend=False, coloraxis_showscale=False,
                           margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig2, use_container_width=True)

    with tab2:
        st.subheader("In-Play Flat-Stake Simulation (1 trade per match)")
        sim = edge_live.copy()
        st.dataframe(
            sim.rename(columns={
                "threshold": "Threshold", "total_trades": "Trades", "wins": "Wins",
                "losses": "Losses", "win_rate": "Win Rate", "profit": "Profit (units)",
                "roi_pct": "ROI %", "max_drawdown_pct": "Max DD %",
            })[["Threshold", "Trades", "Wins", "Losses", "Win Rate", "Profit (units)", "ROI %", "Max DD %"]],
            use_container_width=True, hide_index=True,
        )

        c1, c2 = st.columns(2)
        with c1:
            st.metric("In-Play ROI", f"{float(sim.iloc[0]['roi_pct']):.1f}%")
        with c2:
            st.metric("In-Play Win Rate", f"{float(sim.iloc[0]['win_rate']):.1%}")

    with tab3:
        st.subheader("In-Play Trade Log")
        ip = inplay.sort_values(["date", "innings_number", "over_number"], ascending=[False, True, True])
        display = ip[["date", "batting_team", "bowling_team", "innings_number", "over_number",
                       "model_probability", "market_prob_1", "edge", "market_odds_1", "eventual_winner"]].copy()
        display.columns = ["Date", "Batting", "Bowling", "Inn", "Over",
                           "Model %", "Market %", "Edge", "Odds", "Winner"]
        display["Model %"] = display["Model %"].astype(float).apply(lambda x: f"{x:.1%}")
        display["Market %"] = display["Market %"].astype(float).apply(lambda x: f"{x:.1%}")
        display["Edge"] = display["Edge"].astype(float).apply(lambda x: f"{x:+.1%}")
        display["Won"] = ip["batting_team"].values == ip["eventual_winner"].values
        display["Won"] = display["Won"].map({True: "Yes", False: "No"})
        st.dataframe(display, use_container_width=True, hide_index=True)


def page_bucket_model():
    st.title("Statistical Bucket Model")
    st.markdown("Win probability predictions based on game state buckets: over phase, wickets lost, "
                "run pressure, and Elo differential. Hierarchical stabilization with 5 fallback levels.")

    bm = data["bucket_model"]

    tab1, tab2 = st.tabs(["Model Explorer", "Sample Distribution"])

    with tab1:
        c1, c2, c3, c4 = st.columns(4)
        over_opts = sorted(bm["over_bucket"].unique())
        wicket_opts = sorted(bm["wickets_bucket"].unique())
        pressure_opts = sorted(bm["run_pressure_bucket"].unique())
        elo_opts = sorted(bm["elo_diff_bucket"].unique())

        sel_over = c1.selectbox("Over Phase", over_opts)
        sel_wickets = c2.selectbox("Wickets", wicket_opts)
        sel_pressure = c3.selectbox("Run Pressure", pressure_opts)
        sel_elo = c4.selectbox("Elo Differential", elo_opts)

        filtered = bm[
            (bm["over_bucket"] == sel_over) &
            (bm["wickets_bucket"] == sel_wickets) &
            (bm["run_pressure_bucket"] == sel_pressure) &
            (bm["elo_diff_bucket"] == sel_elo)
        ]

        if len(filtered) > 0:
            row = filtered.iloc[0]
            mc1, mc2, mc3, mc4 = st.columns(4)
            mc1.metric("Win Probability", f"{row['final_stabilized_probability']:.1%}")
            mc2.metric("Sample Size", f"{int(row['sample_size']):,}")
            mc3.metric("Raw Win %", f"{row['win_probability']:.1%}")
            mc4.metric("Fallback Level", int(row["fallback_level"]))

            if row["sample_size"] > 0:
                st.progress(float(row["final_stabilized_probability"]))
        else:
            st.info("No data for this combination.")

    with tab2:
        st.subheader("Sample Size Distribution")
        fig = px.histogram(bm, x="sample_size", nbins=50,
                           labels={"sample_size": "Sample Size"},
                           color_discrete_sequence=["#1f77b4"])
        fig.update_layout(height=350, margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig, use_container_width=True)

        c1, c2, c3 = st.columns(3)
        c1.metric("Total Buckets", len(bm))
        c2.metric("Median Sample Size", int(bm["sample_size"].median()))
        c3.metric("Fallback Level Distribution",
                  ", ".join(f"L{int(k)}:{int(v)}" for k, v in bm["fallback_level"].value_counts().sort_index().items()))

        st.subheader("Win Probability by Over Phase and Elo")
        heatmap = bm.groupby(["over_bucket", "elo_diff_bucket"])["final_stabilized_probability"].mean().unstack()
        fig2 = px.imshow(
            heatmap, text_auto=".1%", color_continuous_scale="RdYlGn",
            labels={"x": "Elo Differential", "y": "Over Phase", "color": "Win Prob"},
            aspect="auto",
        )
        fig2.update_layout(height=400, margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig2, use_container_width=True)


PAGES = {
    "Overview": page_overview,
    "Elo Ratings": page_elo,
    "Model Calibration": page_calibration,
    "Pre-Match Edge": page_pre_match_edge,
    "In-Play Edge": page_inplay_edge,
    "Bucket Model": page_bucket_model,
}

PAGES[page]()
