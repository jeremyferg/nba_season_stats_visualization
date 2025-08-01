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
from sqlalchemy import create_engine

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
username = 'root'
password = 'zKq40Bnc!'
host = 'localhost'
database = 'nba_stats'

# create engine
engine = create_engine(f"mysql+pymysql://{username}:{password}@{host}/{database}")

# get a list of the tables
tables_list = [player_info, 
               player_seasons, 
               player_career, 
               awards, 
               player_awards,
               player_scoreboards,
               team_info,
               team_seasons,
               team_scoreboards,
               lineups,
               arenas,
               games,
               broadcasters]
# populate the SQL tables with the CSV files
for table in tables_list:
    table.to_sql(table.__name__, con=engine, if_exists='append', index=False)
