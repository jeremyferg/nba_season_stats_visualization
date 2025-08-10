##########################
##########################
#####                #####
#####   csv_to_sql   #####
#####                #####
##########################
##########################

# transfer all csv tables to sql database

#############################################################################################################################

############################
###                      ###
###  LIBRARIES/DATASETS  ###
###                      ###
############################

import pandas as pd
from sqlalchemy import create_engine, text
from tqdm import tqdm

# player_info
player_info_500 = pd.read_csv('data/processed/player_scoreboard/player_info_500.csv')
player_info_1000 = pd.read_csv('data/processed/player_scoreboard/player_info_1000.csv')
player_info_1500 = pd.read_csv('data/processed/player_scoreboard/player_info_1500.csv')

player_info = pd.concat([player_info_500, player_info_1000, player_info_1500], ignore_index=True)

# player_seasons
player_seasons_500 = pd.read_csv("data/processed/player_scoreboard/player_seasons_500.csv")
player_seasons_1000 = pd.read_csv("data/processed/player_scoreboard/player_seasons_1000.csv")
player_seasons_1500 = pd.read_csv("data/processed/player_scoreboard/player_seasons_1500.csv")

player_seasons = pd.concat([player_seasons_500, player_seasons_1000, player_seasons_1500], ignore_index=True)

# player_career
player_career_500 = pd.read_csv("data/processed/player_scoreboard/player_career_500.csv")
player_career_1000 = pd.read_csv("data/processed/player_scoreboard/player_career_1000.csv")
player_career_1500 = pd.read_csv("data/processed/player_scoreboard/player_career_1500.csv")

player_career = pd.concat([player_career_500, player_career_1000, player_career_1500], ignore_index=True)

# awards
awards_500 = pd.read_csv("data/processed/player_scoreboard/awards_500.csv")
awards_1000 = pd.read_csv("data/processed/player_scoreboard/awards_1000.csv")
awards_1500 = pd.read_csv("data/processed/player_scoreboard/awards_1500.csv")

awards = pd.concat([awards_500, awards_1000, awards_1500], ignore_index=True)
awards = awards.drop_duplicates(subset=['award_name'])

# player_awards
player_awards_500 = pd.read_csv("data/processed/player_scoreboard/player_awards_500.csv")
player_awards_1000 = pd.read_csv("data/processed/player_scoreboard/player_awards_1000.csv")
player_awards_1500 = pd.read_csv("data/processed/player_scoreboard/player_awards_1500.csv")

player_awards = pd.concat([player_awards_500, player_awards_1000, player_awards_1500], ignore_index=True)

# player_scoreboards
player_scoreboards = pd.read_csv("data/processed/player_scoreboard/player_scoreboards.csv")

# team_info
team_info = pd.read_csv("data/processed/team_scoreboard/team_info.csv")

# team_seasons
team_seasons = pd.read_csv("data/processed/team_scoreboard/team_seasons.csv")

# team_scoreboards
team_scoreboards = pd.read_csv("data/processed/team_scoreboard/team_scoreboards.csv")

# lineups
lineups = pd.read_csv("data/processed/team_scorebord/lineups.csv")

# arenas
arenas = pd.read_csv("data/processed/games/arenas.csv")

# games
games = pd.read_csv("data/processed/games/games.csv")

# broadcasters
broadcasters = pd.read_csv("data/processed/games/broadcasters.csv")

#############################################################################################################################

# replace credentials and connection info as needed
username = ''
password = ''
host = ''
database = ''

# create engine
engine = create_engine(f"mysql+pymysql://{username}:{password}@{host}/{database}")

# first tables before scoreboards (specifically games) must be populated
tables_list_pre_scoreboards = [
    ("player_info", player_info),
    ("team_info", team_info),
    ("arenas", arenas),
    ("broadcasters", broadcasters),
    ("games", games)
]

for table_name, table_df in tqdm(tables_list_pre_scoreboards):
    table_df.to_sql(table_name, con=engine, if_exists='append', index=False)

# oddly, some rows of scoreboard data are missng game IDs, these rows should be dropped
# in order to accomplish this, query SQL for a list of all the valid game IDs
with engine.connect() as conn:
    existing_game_ids = set(
        row[0] for row in conn.execute(text("SELECT game_id FROM games"))
    )

# populate the rest of the database
tables_list_post_scoreboards = [
    ("player_career", player_career),
    ("player_seasons", player_seasons),
    ("awards", awards),
    ("player_awards", player_awards),
    ("player_scoreboards", player_scoreboards),
    ("team_seasons", team_seasons),
    ("team_scoreboards", team_scoreboards)
]

for table_name, table_df in tqdm(tables_list_post_scoreboards):
    df_to_insert = table_df

    # Special handling for player_scoreboards to filter invalid game_ids
    if table_name == "player_scoreboards" or table_name == "team_scoreboards":
        df_to_insert = table_df[table_df["game_id"].isin(existing_game_ids)].copy()
        dropped = len(table_df) - len(df_to_insert)
        print(f"[INFO] Dropped {dropped} rows from {table_name} due to missing game_id.")

    # insert into the table
    df_to_insert.to_sql(table_name, con=engine, if_exists='append', index=False)

# similar to game IDs, some players in the lineups results to not have player IDs
# so we drop these results    
print('Executing lineups...')

# get all valid player_ids from player_info
with engine.connect() as conn:
    result = conn.execute(text("SELECT player_id FROM player_info"))
    valid_player_ids = {row[0] for row in result.fetchall()}

# define a helper function to check all players exist
def all_players_exist(row):
    return all(pid in valid_player_ids for pid in [row['player_1'], row['player_2'], row['player_3'], row['player_4'], row['player_5']])

# filter lineups
filtered_lineups_df = lineups[lineups.apply(all_players_exist, axis=1)].copy()

# now insert only valid rows
filtered_lineups_df.to_sql('lineups', con=engine, if_exists='append', index=False)