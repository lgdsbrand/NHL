import pandas as pd
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# =========================
# 🔑 CONFIG
# =========================

KEY = "38f3ae4f013496ded18e75b7f9c49ae9"
SHEET_NAME = "NHL Vercel"

# =========================
# 🗺️ TEAM NAME MAP
# =========================

TEAM_MAP = {
    "Boston": "Boston Bruins",
    "New York": "New York Rangers",
    "Vegas": "Vegas Golden Knights",
    "Los Angeles": "Los Angeles Kings",
    "New Jersey": "New Jersey Devils",
    "Montreal": "Montreal Canadians",
    "Tampa Bay": "Tampa Bay Lightning",
    "Buffalo": "Buffalo Sabres",
    "Utah": "Utah Mammoth",
    "Colorado": "Colorado Avalanche"
    # add more as needed
}

# =========================
# 🔗 GOOGLE SHEETS CONNECT
# =========================

def connect_sheet():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = ServiceAccountCredentials.from_json_keyfile_name(
        "credentials.json", scope
    )

    client = gspread.authorize(creds)
    return client.open_by_url("https://docs.google.com/spreadsheets/d/1aAmugvWUMDxw9J7jp4zcB4ccUZxPmeRAZOfcFmDSnwI/edit?gid=0#gid=0")


def push_to_sheet(df, tab_name):
    sheet = connect_sheet().worksheet(tab_name)
    sheet.clear()
    sheet.update([df.columns.values.tolist()] + df.values.tolist())


# =========================
# 🏒 ESPN GAMES
# =========================

def get_games():
    url = "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard"
    data = requests.get(url).json()

    games = []

    for event in data["events"]:
        comp = event["competitions"][0]["competitors"]

        home = [t for t in comp if t["homeAway"] == "home"][0]
        away = [t for t in comp if t["homeAway"] == "away"][0]

        away_team = TEAM_MAP.get(
            away["team"]["displayName"],
            away["team"]["displayName"]
        )

        home_team = TEAM_MAP.get(
            home["team"]["displayName"],
            home["team"]["displayName"]
        )

        games.append({
            "away_team": away_team,
            "home_team": home_team,
            "time": event["date"]
        })

    return pd.DataFrame(games)


# =========================
# 📊 NST TEAM STATS
# =========================

def get_team_stats():
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
# 🥅 NST GOALIES
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
# 🏆 STANDINGS
# =========================

def get_standings():
    url = (
        "https://data.naturalstattrick.com/teamtable.php?"
        f"sit=all&score=all&rate=n&team=all&loc=B&key={KEY}"
    )

    df = pd.read_html(url)[0]
    df.columns = df.columns.droplevel(0) if isinstance(df.columns, pd.MultiIndex) else df.columns

    return df[[
        "Team", "PTS%"
    ]].rename(columns={"Team": "team"})


# =========================
# 🔗 BUILD MATCHUPS
# =========================

def build_games():
    games = get_games()
    stats = get_team_stats()
    standings = get_standings()

    df = games.merge(stats, left_on="away_team", right_on="team", how="left")
    df = df.rename(columns={
        "GF/60": "away_GF60",
        "GA/60": "away_GA60",
        "xGF/60": "away_xGF60",
        "xGA/60": "away_xGA60"
    }).drop(columns=["team"])

    df = df.merge(stats, left_on="home_team", right_on="team", how="left")
    df = df.rename(columns={
        "GF/60": "home_GF60",
        "GA/60": "home_GA60",
        "xGF/60": "home_xGF60",
        "xGA/60": "home_xGA60"
    }).drop(columns=["team"])

    df = df.merge(standings, left_on="away_team", right_on="team", how="left")
    df = df.rename(columns={"PTS%": "away_pts_pct"}).drop(columns=["team"])

    df = df.merge(standings, left_on="home_team", right_on="team", how="left")
    df = df.rename(columns={"PTS%": "home_pts_pct"}).drop(columns=["team"])

    return df


# =========================
# 🚀 RUN PIPELINE
# =========================

df_games = build_games()
df_goalies = get_goalies()

# Push to Google Sheets
push_to_sheet(df_games, "Games")
push_to_sheet(df_goalies, "Goalies")

print("✅ Data pushed to Google Sheets")
