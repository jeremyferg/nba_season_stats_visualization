#############################
#############################
#####                   #####
#####   get_raw_games   #####
#####                   #####
#############################
#############################

# get raw information about games
# used to create the games, arenas, and broadcasters tables

#############################################################################################################################

##########################################
###                                    ###
###  LIBRARIES/DATASETS/CONFIGURATION  ###
###                                    ###
##########################################

import os
import time
import numpy as np
import pandas as pd
from tqdm import tqdm
from nba_api.stats.endpoints import scheduleleaguev2, winprobabilitypbp
from chunk_helper import get_completed_chunks, mark_chunk_completed, load_chunk

chunk_dir = "data/raw/games"
output_dir = "data/raw/games"
checkpoint_file = "data/raw/games/completed_chunks.txt"

os.makedirs(output_dir, exist_ok=True)

#############################################################################################################################

#######################
###                 ###
###  GAMES_EXTRACT  ###
###                 ###
#######################

# get the raw information needed to create game-related tables
# NOTE: broadcaster/arena game information only starts in 2017
# so areanas, and broadcasters tables are only comprehensive to those years

def games_extract(games_id_list, chunk_id=None, save_dir="data/raw/games"):
    
    seasons = list(range(2017, 2025))
    df = pd.DataFrame()
    failed = []

    # get game information for each season
    for season in tqdm(seasons, desc="Fetching schedule info"):
        try:
            game_df = scheduleleaguev2.ScheduleLeagueV2(season=season).get_data_frames()[0]
            df = pd.concat([df, game_df], ignore_index=True)
            time.sleep(.4)
        except Exception as e:
            print(f"Failed to fetch Year {season}: {e}")
            failed.append(season)
            time.sleep(.4)

    # if the API fails once, retry the ID at the end of the loop
    for season in tqdm(failed, desc="Retrying failed games"):
        attempts = 0
        # two extra attempts per ID
        while attempts < 2:
            try:
                game_df = scheduleleaguev2.ScheduleLeagueV2(season=season).get_data_frames()[0]
                df = pd.concat([df, game_df], ignore_index=True)
                time.sleep(.4)
                break
            except Exception as e:
                print(f"Retry failed for season {season}, retrying in 3 seconds: {e}")
                attempts += 1
                time.sleep(.4)

    # get the specific columns needed for the processed dataframes
    df = df[['gameId', 'gameDate', 'seasonYear', 'arenaName', 'arenaCity', 'arenaState',
             'homeTeam_teamTricode', 'awayTeam_teamTricode',
             'nationalBroadcasters_broadcasterId', 'nationalBroadcasters_broadcasterDisplay',
             'homeTvBroadcasters_broadcasterId', 'homeTvBroadcasters_broadcasterDisplay',
             'awayTvBroadcasters_broadcasterId', 'awayTvBroadcasters_broadcasterDisplay']]

    winprobs_df = pd.DataFrame()
    failed = []

    # get win probability information based on game IDs
    for game_id in tqdm(games_id_list, desc=f"Fetching games (chunk {chunk_id})"):
        try:
            winprob_df = winprobabilitypbp.WinProbabilityPBP(game_id=game_id)
            winprob_df_0 = winprob_df.get_data_frames()[0].iloc[[0]]
            winprob_df_1 = winprob_df.get_data_frames()[1]
            winprob_merged = pd.merge(winprob_df_0, winprob_df_1, on="GAME_ID", how="inner")
            winprobs_df = pd.concat([winprobs_df, winprob_merged], ignore_index=True)
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
                winprob_df = winprobabilitypbp.WinProbabilityPBP(game_id=game_id)
                winprob_df_0 = winprob_df.get_data_frames()[0].iloc[[0]]
                winprob_df_1 = winprob_df.get_data_frames()[1]
                winprob_merged = pd.merge(winprob_df_0, winprob_df_1, on="GAME_ID", how="inner")
                winprobs_df = pd.concat([winprobs_df, winprob_merged], ignore_index=True)
                time.sleep(.4)
                break
            except Exception as e:
                print(f"Retry failed for Game ID {game_id}, retrying in 3 seconds: {e}")
                attempts += 1
                time.sleep(.4)

    # get the specific columns needed for the processed dataframes
    winprobs_df = winprobs_df[['GAME_ID', 'GAME_DATE', 'HOME_TEAM_ID', 'HOME_TEAM_ABR',
                               'VISITOR_TEAM_ID', 'VISITOR_TEAM_ABR', 'HOME_TEAM_PTS', 'VISITOR_TEAM_PTS']]

    # create an outcomes column
    conditions = [
        (winprobs_df["HOME_TEAM_PTS"] > winprobs_df["VISITOR_TEAM_PTS"]),
        (winprobs_df["HOME_TEAM_PTS"] < winprobs_df["VISITOR_TEAM_PTS"])
    ]
    choices = [
        winprobs_df["HOME_TEAM_ABR"] + " Wins",
        winprobs_df["VISITOR_TEAM_ABR"] + " Wins"
    ]
    winprobs_df["OUTCOME"] = np.select(conditions, choices, winprobs_df["OUTCOME"])

    # merge and rename the columns together
    games_merged = pd.merge(winprobs_df, df, left_on="GAME_ID", right_on="gameId", how="left")
    games_merged = games_merged.rename(columns={
        'arenaName': 'ARENA_NAME',
        'arenaCity': 'ARENA_CITY',
        'arenaState': 'ARENA_STATE',
        'nationalBroadcasters_broadcasterId': 'NAT_BROADCASTER_ID',
        'nationalBroadcasters_broadcasterDisplay': 'NAT_BROADACSTER_NAME',
        'homeTvBroadcasters_broadcasterId': 'HOME_BROADCASTER_ID',
        'homeTvBroadcasters_broadcasterDisplay': 'HOME_BROADACSTER_NAME',
        'awayTvBroadcasters_broadcasterId': 'VISITOR_BROADCASTER_ID',
        'awayTvBroadcasters_broadcasterDisplay': 'VISITOR_BROADACSTER_NAME'
    })
    # lower all the column names
    games_merged.columns = map(str.lower, games_merged.columns)
    # place the chunk into the correct folder as a JSON 
    if chunk_id is not None:
        output_path = os.path.join(save_dir, f"games_results_chunk_{chunk_id}.json")
        games_merged.to_json(output_path, orient="records", indent=2)
    
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
        for f in os.listdir(chunk_dir)
        if f.startswith("games_chunk_") and f.endswith(".json")
    ]
    completed_chunks = get_completed_chunks(checkpoint_file)
    remaining_chunks = sorted(set(all_chunks) - completed_chunks)

    # if all the chunks have been processed, yay!
    if not remaining_chunks:
        print("All chunks processed!")
        return

    # run a chunk
    for chunk_id in remaining_chunks[:1]:
        print(f"Processing chunk {chunk_id}...")
        game_ids = load_chunk(chunk_dir, chunk_id)
        games_extract(game_ids, chunk_id=chunk_id, save_dir=output_dir)
        mark_chunk_completed(checkpoint_file, chunk_id)
        print(f"Finished chunk {chunk_id}")
        break

#############################################################################################################################

if __name__ == "__main__":
    main()

#############################################################################################################################