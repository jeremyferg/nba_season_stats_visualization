##########################################
##########################################
#####                                #####
#####   get_raw_player_scoreboards   #####
#####                                #####
##########################################
##########################################

# get basic statistics on box scores per game
# the API endpoint needed requires player IDs whereas other related boxscores use
    # game IDs, hence this is considered the raw/initial version of the table

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

from nba_api.stats.endpoints import leaguegamefinder

output_path = "data/raw/player_scoreboard/player_ids.json"
# Read the JSON data
with open(output_path, 'r') as outfile:
    player_ids = json.load(outfile)

#############################################################################################################################

################################
###                          ###
###  PLAYER_SCOREBOARDS_RAW  ###
###                          ###
################################

# get basic statistics on box scores per game
# returns a DataFrame of those statistics
# the API endpoint needed requires player IDs whereas other related boxscores use
    # game IDs, hence this is considered the raw/initial version of the table

def player_scoreboards_raw(player_ids):

    df = pd.DataFrame()
    failed = []
    # get the boxscores of specific players 
    for player_id in tqdm(player_ids, desc="Fetching player games"):
        try:
            response = leaguegamefinder.LeagueGameFinder(player_id_nullable=player_id)
            player_df = response.get_data_frames()[0]
            player_df['PLAYER_ID']=player_id
            df = pd.concat([df, player_df], ignore_index=True)
            time.sleep(.4)

        except Exception as e:
            print(f"Failed to fetch Player ID {player_id}: {e}")
            failed.append(player_id)
            time.sleep(.4)

    # if the API fails once, retry the ID at the end of the loop
    for player_id in tqdm(failed, desc="Retrying failed games"):
        attempts = 0
        # two extra attempts per ID
        while attempts < 2:
            try:
                response = leaguegamefinder.LeagueGameFinder(player_id_nullable=player_id)
                player_df = response.get_data_frames()[0]
                player_df['PLAYER_ID']=player_id
                df = pd.concat([df, player_df], ignore_index=True)
                time.sleep(.4)
                break

            except Exception as e:
                print(f"Retry failed for player id {player_id}, retrying in 3 seconds: {e}")
                attempts += 1
                time.sleep(.4)

    # only filter for the games being examined
    df['GAME_DATE'] = pd.to_datetime(df['GAME_DATE'], errors='coerce')
    df = df[
        (df['GAME_DATE'].dt.month != 7) & 
        (df['GAME_DATE'] > pd.to_datetime('2014-10-14'))
    ]

    return df

#############################################################################################################################

##############
###        ###
###  MAIN  ###
###        ###
##############

def main():

    # split player ID list into three parts to avoid the API from completely timing out
    # script needs to be ran three times to fully finish

    if not os.path.exists("data/raw/player_scoreboard/player_boxscore_raw_500_df.csv"):
        print("Building raw boxscore tables (1-500).")
        player_boxscore_raw_df = player_scoreboards_raw(player_ids[:500])
        print("Built player seasons tables (1-500).")
        player_boxscore_raw_df.to_csv("data/raw/player_scoreboard/player_boxscore_raw_500_df.csv", index=False)

    elif not os.path.exists("data/raw/player_scoreboard/player_boxscore_raw_1000_df.csv"):
        print("Building raw boxscore tables (501-1000).")
        player_boxscore_raw_df = player_scoreboards_raw(player_ids[500:1000])
        print("Built player seasons tables (501-1000).")
        player_boxscore_raw_df.to_csv("data/raw/player_scoreboard/player_boxscore_raw_1000_df.csv", index=False)

    elif not os.path.exists("data/raw/player_scoreboard/player_boxscore_raw_1500_df.csv"):
        print("Building raw boxscore tables (1001+).")
        player_boxscore_raw_df = player_scoreboards_raw(player_ids[1000:])
        print("Built player seasons tables(1001+).")
        player_boxscore_raw_df.to_csv("data/raw/player_scoreboard/player_boxscore_raw_1500_df.csv", index=False)

    else:
        print("All player boxscores extracted.")

#############################################################################################################################

if __name__ == "__main__":
    main()