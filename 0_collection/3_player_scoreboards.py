##################################
##################################
#####                        #####
#####   player_scoreboards   #####
#####                        #####
##################################
##################################

# gets more detailed JSON chunks of player boxscores

#############################################################################################################################

##########################################
###                                    ###
###  LIBRARIES/DATASETS/CONFIGURATION  ###
###                                    ###
##########################################

import os
import json
import time
import pandas as pd
from tqdm import tqdm
from nba_api.stats.endpoints import boxscoreadvancedv3, boxscoreplayertrackv3

player_boxscore_raw_500_df = pd.read_csv('data/raw/player_scoreboard/player_boxscore_raw_500_df.csv')
player_boxscore_raw_1000_df = pd.read_csv('data/raw/player_scoreboard/player_boxscore_raw_1000_df.csv')
player_boxscore_raw_1500_df = pd.read_csv('data/raw/player_scoreboard/player_boxscore_raw_1500_df.csv')
player_boxscore_raw_df = pd.concat(
    [player_boxscore_raw_500_df, 
     player_boxscore_raw_1000_df,
     player_boxscore_raw_1500_df
     ], 
     ignore_index=True)

CHUNK_DIR = "data/raw/games"
OUTPUT_DIR = "data/raw/player_scoreboard"
CHECKPOINT_FILE = "data/raw/player_scoreboard/completed_chunks.txt"

os.makedirs(OUTPUT_DIR, exist_ok=True)

#############################################################################################################################

##############################
###                        ###
###  GET_COMPLETED_CHUNKS  ###
###                        ###
##############################

# helper function that returns a list of extracted chunks

def get_completed_chunks():
    if not os.path.exists(CHECKPOINT_FILE):
        return set()
    with open(CHECKPOINT_FILE) as f:
        return set(int(line.strip()) for line in f if line.strip())

#############################################################################################################################

##############################
###                        ###
###  MARK_CHUNK_COMPLETED  ###
###                        ###
##############################

# helper function that marks a chunk as complete

def mark_chunk_completed(chunk_id):
    with open(CHECKPOINT_FILE, "a") as f:
        f.write(f"{chunk_id}\n")

#############################################################################################################################

####################
###              ###
###  LOAD_CHUNK  ###
###              ###
####################

# helper function that loads the correct JSON file based on the required chunk

def load_chunk(chunk_id):
    with open(os.path.join(CHUNK_DIR, f"games_chunk_{chunk_id}.json"), "r") as f:
        return json.load(f)

#############################################################################################################################

###########################
###                     ###
###  PLAYER_SCOREBOARD  ###
###                     ###
###########################

# takes a list of game IDs and raw boxscores, adds more information to these scores, and returns a JSON of this information
# each chunk of game IDs is currently 250

def player_scoreboard(games_id_list, players_boxscore_raw_df, chunk_id=None, save_dir="data/raw/player_scoreboard"):


    game_player_track_df = pd.DataFrame()
    game_player_advanced_df = pd.DataFrame()
    failed = []
    
    # get the boxscores of specific games
    for game_id in tqdm(games_id_list, desc="Fetching player game boxscores"):
        try:
            response_1 = boxscoreplayertrackv3.BoxScorePlayerTrackV3(game_id=game_id)
            time.sleep(1)          
            response_2 = boxscoreadvancedv3.BoxScoreAdvancedV3(game_id=game_id)
            
            game_df_1 = response_1.get_data_frames()[0]
            game_df_2 = response_2.get_data_frames()[0]
            
            game_player_track_df = pd.concat([game_player_track_df, game_df_1], ignore_index=True)
            game_player_advanced_df = pd.concat([game_player_advanced_df, game_df_2], ignore_index=True)

            time.sleep(.4)

        except Exception as e:
            print(f"Failed to fetch Game ID {game_id}: {e}")
            failed.append(game_id)
            time.sleep(.4)

    # if the API fails once, retry the ID at the end of the loop
    for game_id in tqdm(failed, desc="Retrying failed games"):
        attempts = 0
        # two extra attempts per ID
        while attempts < 2:
            try:
               response_1 = boxscoreplayertrackv3.BoxScorePlayerTrackV3(game_id=game_id)
               time.sleep(1)       
               response_2 = boxscoreadvancedv3.BoxScoreAdvancedV3(game_id=game_id)
               
               game_df_1 = response_1.get_data_frames()[0]
               game_df_2 = response_2.get_data_frames()[0]
               
               game_player_track_df = pd.concat([game_player_track_df, game_df_1], ignore_index=True)
               game_player_advanced_df = pd.concat([game_player_advanced_df, game_df_2], ignore_index=True)
               
               time.sleep(.4)
               break
            except Exception as e:
                print(f"Retry failed for game id {game_id}, retrying in 3 seconds: {e}")
                attempts += 1
                time.sleep(.4)

    # merge the dataframes extracted from this function
    player_boxscore_merged = pd.merge(game_player_track_df, 
                        game_player_advanced_df, 
                        on=["gameId", "personId"],
                        how="inner")
    player_boxscore_merged['gameId'] = player_boxscore_merged['gameId'].astype("int64")
    # merge the new dataframe with the raw dataframe
    player_games = pd.merge(players_boxscore_raw_df, 
                        player_boxscore_merged, 
                        left_on=["PLAYER_ID", "GAME_ID"],
                        right_on=["personId", "gameId"],
                        how="inner")

    # drop and rename columns
    player_games = player_games.drop([
        "TEAM_NAME",
        "MIN", 
        "SEASON_ID", 
        "TEAM_ID", 
        "TEAM_ABBREVIATION", 
        "USE", 
        "EST_USE_PCT",
        "gameId", 
        "personId",
        "assists",
        "fieldGoalPercentage",
        "teamId_x",
        "teamCity_x",
        "teamName_x",
        "teamTricode_x",
        "teamSlug_x",
        "firstName_x",
        "familyName_x",
        "nameI_x",
        "playerSlug_x",
        "position_x",
        "comment_x",
        "jerseyNum_x",
        "minutes_y",
        "teamId_y",
        "teamCity_y",
        "teamName_y",
        "teamTricode_y",
        "teamSlug_y",
        "firstName_y",
        "familyName_y",
        "nameI_y",
        "playerSlug_y",
        "position_y",
        "comment_y",
        "jerseyNum_y"
        "usagePercentage",
        "estimatedUsagePercentage"
    ],
                                    axis=1)
    player_games = player_games.rename({
        "WL": "winlose",
        "minutes_x": "MINUTES",
        "speed": "SPEED",
        "distance": "DISTANCE",
        "reboundChancesOffensive": "REB_CHANCES_OFF",
        "reboundChancesDefensive": "REB_CHANCES_DEF",
        "reboundChancesTotal": "REB_CHANCES_TOTAL",
        "touches": "TOUCHES",
        "secondaryAssists": "SECONDARY_AST",
        "freeThrowAssists": "FT_AST",
        "passes": "PASSES",
        "contestedFieldGoalsMade": "CONTESTED_FGM",
        "contestedFieldGoalsAttempted": "CONTESTED_FGA",
        "contestedFieldGoalPercentage": "CONTESTED_FG_PCT",
        "uncontestedFieldGoalsMade": "UNCONTESTED_FGM",
        "uncontestedFieldGoalsAttempted": "UNCONTESTED_FGA",
        "uncontestedFieldGoalsPercentage": "UNCONTESTED_FG_PCT",
        "defendedAtRimFieldGoalsMade": "DEFENDED_AT_RIM_FGM",
        "defendedAtRimFieldGoalsAttempted": "DEFENDED_AT_RIM_FGA",
        "defendedAtRimFieldGoalPercentage": "DEFENDED_AT_FG_PCT",
        "estimatedOffensiveRating": "EST_OFF_RATING", 
        "offensiveRating": "OFF_RATING", 
        "estimatedDefensiveRating": "EST_DEF_RATING", 
        "defensiveRating": "DEF_RATING", 
        "estimatedNetRating": "EST_NET_RATING", 
        "netRating": "NET_RATING", 
        "assistPercentage": "AST_PCT", 
        "assistToTurnover": "AST_TO_TOV", 
        "assistRatio": "AST_RATIO", 
        "offensiveReboundPercentage": "OFF_REB_PCT", 
        "defensiveReboundPercentage": "DEF_REB_PCT", 
        "reboundPercentage": "REB_PCT", 
        "turnoverRatio": "TOV_RATIO", 
        "effectiveFieldGoalPercentage": "EFFECTIVE_FG_PCT", 
        "trueShootingPercentage": "TS_PCT", 
        "estimatedPace": "EST_PACE", 
        "pace": "PACE", 
        "pacePer40": "PACE_PER_40", 
        "possessions": "POSSESSIONS" 
    },
                                      axis=1)

    # mover player IDs column
    cols = list(player_games.columns)
    cols.insert(0, cols.pop(cols.index('PLAYER_ID')))
    player_games = player_games[cols]
    # lower all the column names
    player_games.columns = map(str.lower, player_games.columns)
    # place the chunk into the correct folder as a JSON
    if chunk_id is not None:
        output_path = os.path.join(save_dir, f"games_results_chunk_{chunk_id}.json")
        player_games.to_json(output_path, orient="records", indent=2)
    
#############################################################################################################################

##############
###        ###
###  MAIN  ###
###        ###
##############

def main():

    # check and see how many chunks have been completed and how many remain
    all_chunks = [
        int(f.split("_")[-1].split(".")[0])
        for f in os.listdir(CHUNK_DIR)
        if f.startswith("games_chunk_") and f.endswith(".json")
    ]
    completed_chunks = get_completed_chunks()
    remaining_chunks = sorted(set(all_chunks) - completed_chunks)

    # if all the chunks have been processed, yay!
    if not remaining_chunks:
        print("All chunks processed!")
        return

    # run a chunk 
    for chunk_id in remaining_chunks[:1]:
        print(f"Processing chunk {chunk_id}...")
        game_ids = load_chunk(chunk_id)
        player_scoreboard(game_ids, player_boxscore_raw_df, chunk_id=chunk_id, save_dir=OUTPUT_DIR) 
        mark_chunk_completed(chunk_id)
        print(f"Finished chunk {chunk_id}")
        break

#############################################################################################################################

if __name__ == "__main__":
    main()

#############################################################################################################################