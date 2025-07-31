############################
############################
#####                  #####
#####   get_id_lists   #####
#####                  #####
############################
############################

# gets the player, team, and  game IDs
# used to query through endpoints in the API

#############################################################################################################################

############################
###                      ###
###  LIBRARIES/DATASETS  ###
###                      ###
############################

import os
import json
import time
import pandas as pd
from tqdm import tqdm
from nba_api.stats.static import players, teams
from nba_api.stats.endpoints import leaguegamefinder

with open("data/raw/unique_players_2014_2024.txt", "r", encoding="utf-8") as f:
    season_players = [line.strip() for line in f]

#############################################################################################################################

########################
###                  ###
###  GET_PLAYERS_ID  ###
###                  ###
########################

# finds the IDs of all season plauyers
# returns a list of player IDs
# NOTE: some players are not avaiable in the NBA API  

def get_players_id():
    active_players_id = []
    for player in season_players:
        try:
            # find the IDs using NBA API
            active_player = players.find_players_by_full_name(player)
            player_id = active_player[0].get("id")
            active_players_id.append(player_id)
        except Exception as e:
            print(f"Failed to get ID for player: {player} — Error: {e}")

    return active_players_id

#############################################################################################################################

######################
###                ###
###  GET_TEAMS_ID  ###
###                ###
######################

# finds the IDs of teams
# returns a list of team IDs

def get_teams_id():

    # collect information from NBA API
    nba_teams = teams.get_teams()

    # get exclusively the IDs
    teams_id = []
    for team in nba_teams:
        team_id = team.get("id")
        teams_id.append(team_id)

    return teams_id

#############################################################################################################################

######################
###                ###
###  GET_GAMES_ID  ###
###                ###
######################

# finds the IDs of games from the 2014-15 season to the 2024-25 season
# returns a list of team IDs

def get_games_id():
    
    # get the team IDs
    teams_id = get_teams_id()
    # start an empty DataFrame
    df = pd.DataFrame()

    # collect all games from each team
    for team_id in tqdm(teams_id, desc="Fetching team games"):
        try:
            response = leaguegamefinder.LeagueGameFinder(team_id_nullable=team_id)
            team_df = response.get_data_frames()[0]
            df = pd.concat([df, team_df], ignore_index=True)
            time.sleep(.3)

        except Exception as e:
            print(f"Failed to fetch Team ID {team_id}: {e}")
            raise
    
    # filter down to games for the 2014-15 season onward
    df['GAME_DATE'] = pd.to_datetime(df['GAME_DATE'], errors='coerce')
    df = df[
        (df['GAME_DATE'].dt.month != 7) & 
        (df['GAME_DATE'] > pd.to_datetime('2014-10-14'))
        ]

    #change the column of interest into a list
    games_id_list = df['GAME_ID'].unique().tolist()

    return games_id_list

#############################################################################################################################

############################
###                      ###
###  CHUNK_LIST_TO_DICT  ###
###                      ###
############################

# helper function to chunk large lists into smaller pieces
# used for efficient extractions from NBA API
# produced by Chat GPT

def chunk_list_to_dict(lst, chunk_size=250):
    return {i // chunk_size: lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)}

#############################################################################################################################

#############################
###                       ###
###  SAVE_CHUNKS_TO_DISK  ###
###                       ###
#############################

# helper function to save chunks to a specific directory
# produced by Chat GPT

def save_chunks_to_disk(chunks_dict, directory="data/raw/games"):

    for chunk_id, chunk in chunks_dict.items():
        file_path = os.path.join(directory, f"games_chunk_{chunk_id}.json")
        with open(file_path, "w") as f:
            json.dump(chunk, f)

#############################################################################################################################

##############
###        ###
###  MAIN  ###
###        ###
##############

def main():

    print("Extracting PLAYER_IDs...")
    player_ids_list = get_players_id()
    output_path = "data/raw/player_scoreboard/player_ids.json"
    # Write the JSON data
    with open(output_path, 'w', encoding='utf-8') as outfile:
        json.dump(player_ids_list, outfile)
    print("Extracted player ids")

    print("Extracting TEAM_IDs...")
    team_ids_list = get_teams_id()
    output_path_team = "data/raw/team_scoreboard/team_ids.json"
    # Write the JSON data
    with open(output_path_team, 'w', encoding='utf-8') as outfile:
        json.dump(team_ids_list, outfile)
    print("Extracted team ids")
        
    print("Getting full list of GAME_IDs...")
    games_id_list = get_games_id()

    print(f"Total games: {len(games_id_list)}")
    print("Splitting into chunks...")
    chunks = chunk_list_to_dict(games_id_list, chunk_size=250)

    print(f"Saving {len(chunks)} chunks to disk in data/raw/games ...")
    save_chunks_to_disk(chunks, directory="data/raw/games")

    print("Done.")

#############################################################################################################################

if __name__ == "__main__":
    main()

#############################################################################################################################