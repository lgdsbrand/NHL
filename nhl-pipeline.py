import pandas as pd
import requests

KEY = "38f3ae4f013496ded18e75b7f9c49ae9"

# =========================
# 🏒 1. ESPN GAMES
# =========================
def get_games():
    url = "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard"
    data = requests.get(url).json()

    games = []

    for event in data["events"]:
        comp = event["competitions"][0]["competitors"]

        home = [t for t in comp if t["homeAway"] == "home"][0]
        away = [t for t in comp if t["homeAway"] == "away"][0]

        games.append({
            "away_team": away["team"]["displayName"],
            "home_team": home["team"]["displayName"],
            "time": event["date"]
        })

    return pd.DataFrame(games)


# =========================
# 📊 2. NST TEAM STATS
# =========================
def get_nst_team_stats():
    url = (
        "https://data.naturalstattrick.com/teamtable.php?"
        f"sit=5v5&score=all&rate=y&team=all&loc=B&key={KEY}"
    )

    df = pd.read_html(url)[0]
    df.columns = df.columns.droplevel(0) if isinstance(df.columns, pd.MultiIndex) else df.columns

    return df[[
        "Team", "GF/60", "GA/60", "xGF/60", "xGA/60"
    ]].rename(columns={"Team": "team"})


# =========================
# 🥅 3. NST GOALIE STATS
# =========================
def get_goalies():
    url = (
        "https://data.naturalstattrick.com/goalietable.php?"
        f"sit=all&score=all&rate=y&key={KEY}"
    )

    df = pd.read_html(url)[0]
    df.columns = df.columns.droplevel(0) if isinstance(df.columns, pd.MultiIndex) else df.columns

    return df[[
        "Player", "Team", "SV%", "GAA", "GSAA"
    ]].rename(columns={
        "Player": "goalie",
        "Team": "team"
    })


# =========================
# 🏆 4. NST STANDINGS
# =========================
def get_standings():
    url = (
        "https://data.naturalstattrick.com/teamtable.php?"
        f"sit=all&score=all&rate=n&team=all&loc=B&key={KEY}"
    )

    df = pd.read_html(url)[0]
    df.columns = df.columns.droplevel(0) if isinstance(df.columns, pd.MultiIndex) else df.columns

    return df[[
        "Team", "GP", "W", "L", "PTS%"
    ]].rename(columns={"Team": "team"})


# =========================
# 🔗 5. BUILD MATCHUPS
# =========================
def build_games_table():
    games = get_games()
    teams = get_nst_team_stats()
    standings = get_standings()

    # Merge team stats
    df = games.merge(teams, left_on="away_team", right_on="team", how="left")
    df = df.rename(columns={
        "GF/60": "away_GF60",
        "GA/60": "away_GA60",
        "xGF/60": "away_xGF60",
        "xGA/60": "away_xGA60"
    }).drop(columns=["team"])

    df = df.merge(teams, left_on="home_team", right_on="team", how="left")
    df = df.rename(columns={
        "GF/60": "home_GF60",
        "GA/60": "home_GA60",
        "xGF/60": "home_xGF60",
        "xGA/60": "home_xGA60"
    }).drop(columns=["team"])

    # Merge standings (power ranking base)
    df = df.merge(standings, left_on="away_team", right_on="team", how="left")
    df = df.rename(columns={"PTS%": "away_pts_pct"}).drop(columns=["team", "GP", "W", "L"])

    df = df.merge(standings, left_on="home_team", right_on="team", how="left")
    df = df.rename(columns={"PTS%": "home_pts_pct"}).drop(columns=["team", "GP", "W", "L"])

    return df


# =========================
# 🚀 RUN
# =========================

df_games = build_games_table()
df_goalies = get_goalies()

# Save outputs
df_games.to_csv("nhl_games.csv", index=False)
df_goalies.to_csv("nhl_goalies.csv", index=False)

print("Games:")
print(df_games.head())

print("\nGoalies:")
print(df_goalies.head())
