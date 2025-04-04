from nba_api.live.nba.endpoints import scoreboard
from nba_api.stats.endpoints import BoxScoreTraditionalV2
from time import sleep
from rich import print
from sqlalchemy import create_engine, text
import json
from dotenv import load_dotenv
import os

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
TABLE_NAME = os.getenv("TABLE_NAME")


def write_db(json_data, dburl, table_name) -> None:
    """Writes a given bit of JSON to the indicated Postgres database 
table.

    Table must contain a JSONB type column named "raw_json"
    """
    if dburl.startswith("postgresql://") or dburl.startswith("postgres://"):
        url = dburl
    else:
        url = "postgresql://" + dburl
    engine = create_engine(url, future=True)
    con = engine.connect()

    sql = f"""
          INSERT INTO {table_name} (raw_json) VALUES (:jdata);
          """
    for record in json_data:
        con.execute(text(sql), {"jdata": json.dumps(record)})
    con.commit()


def get_todays_NBA_game_ids():
    """Grabs the game ids of today's games."""
    games = scoreboard.ScoreBoard()
    games_dict = games.get_dict()
    print(f"{games_dict['scoreboard']['leagueName']} - {games.score_board_date}")
    return [game["gameId"] for game in games_dict["scoreboard"]["games"]]


def main():
    # Grab todays game ids
    game_ids = get_todays_NBA_game_ids()

    # Store box scores
    all_box_scores = []

    # Check if NBA games were found
    if not game_ids:
        print(" No NBA games found in the given date range.")
    else:
        print(f"Found {len(game_ids)} NBA games today.")

    # Loop through each game and download box scores
    for game_id in game_ids:
        try:
            print(f" Fetching box score for Game ID: {game_id}...")
            # Get box score
            box_score = BoxScoreTraditionalV2(game_id=game_id).get_dict()
            all_box_scores.append(box_score)
            sleep(1) # Sleep to prevent rate limits
        except Exception as e:
            print(f" Error fetching game {game_id}: {e}")

    # Save JSON to DB
    if all_box_scores:
        write_db(all_box_scores, DATABASE_URL, TABLE_NAME)
        print(
            f"[green]Saved NBA box scores for {len(all_box_scores)} games to database.[/green]"
        )
    else:
        print("[red]No box scores were saved.[/red]")


if __name__ == "__main__":
    main()
