################################
################################
#####                      #####
#####   team_scoreboards   #####
#####                      #####
################################
################################

# get raw statistics on team boxscores

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
from nba_api.stats.endpoints import boxscoreadvancedv3, boxscoreplayertrackv3, leaguegamefinder
from chunk_helper import get_completed_chunks, mark_chunk_completed, load_chunk

chunk_dir = "data/raw/games"
output_dir = "data/raw/team_scoreboard"
checkpoint_file = "data/raw/team_scoreboard/completed_chunks.txt"

os.makedirs(output_dir, exist_ok=True)

output_path = "data/raw/team_scoreboard/team_ids.json"
# Read the JSON data
with open(output_path, 'r') as outfile:
    team_ids_list = json.load(outfile)

#############################################################################################################################

##########################
###                    ###
###  TEAM_SCOREBOARDS  ###
###                    ###
##########################

# get basic statistics on box scores per game
# returns a DataFrame of those statistics

def team_scoreboards(games_id_list, chunk_id=None, save_dir="data/raw/team_scoreboard"):
    
    df = pd.DataFrame()
    failed=[]

    # get game information for each team    
    for team_id in tqdm(team_ids_list, desc="Fetching team games"):
        try:
            response = leaguegamefinder.LeagueGameFinder(team_id_nullable=team_id)
            team_df = response.get_data_frames()[0]
            df = pd.concat([df, team_df], ignore_index=True)
            time.sleep(.5)

        except Exception as e:
            print(f"Failed to fetch Team ID {team_id}: {e}")
            time.sleep(.5)

    # if the API fails once, retry the ID at the end of the loop
    for team_id in tqdm(failed, desc="Retrying failed games"):
        attempts = 0
        # two extra attempts per ID
        while attempts < 2:
            try:
                response = leaguegamefinder.LeagueGameFinder(team_id_nullable=team_id)
                team_df = response.get_data_frames()[0]
                df = pd.concat([df, team_df], ignore_index=True)
                time.sleep(.4)
                break
            except Exception as e:
                print(f"Retry failed for Team ID {team_id}, retrying in 3 seconds: {e}")
                attempts += 1
                time.sleep(.4)

    game_team_track_df = pd.DataFrame()
    game_team_advanced_df = pd.DataFrame()
    failed=[]
    
    # get game information for each game ID
    for game_id in tqdm(games_id_list, desc =f"Fetching player game boxscores {chunk_id}"):
        try:
            response_1 = boxscoreplayertrackv3.BoxScorePlayerTrackV3(game_id=game_id)
            time.sleep(.4)       
            response_2 = boxscoreadvancedv3.BoxScoreAdvancedV3(game_id=game_id)
            
            game_df_1 = response_1.get_data_frames()[1]
            game_df_2 = response_2.get_data_frames()[1]
            
            game_team_track_df = pd.concat([game_team_track_df, game_df_1], ignore_index=True)
            game_team_advanced_df = pd.concat([game_team_advanced_df, game_df_2], ignore_index=True)

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
                time.sleep(.4)         
                response_2 = boxscoreadvancedv3.BoxScoreAdvancedV3(game_id=game_id)
            
                game_df_1 = response_1.get_data_frames()[1]
                game_df_2 = response_2.get_data_frames()[1]

                game_team_track_df = pd.concat([game_team_track_df, game_df_1], ignore_index=True)
                game_team_advanced_df = pd.concat([game_team_advanced_df, game_df_2], ignore_index=True)
                time.sleep(.4)
                break
            except Exception as e:
                print(f"Retry failed for Game ID {game_id}, retrying in 3 seconds: {e}")
                attempts += 1
                time.sleep(.4)

    # merge the dataframes together
    team_boxscore_merged = pd.merge(game_team_track_df, 
                        game_team_advanced_df, 
                        on=["gameId", "teamId"],
                        how="inner")
    team_games = pd.merge(df, 
                        team_boxscore_merged, 
                        left_on=["TEAM_ID", "GAME_ID"],
                        right_on=["teamId", "gameId"],
                        how="inner")

    # drop and rename columns
    team_games = team_games.drop([
        "SEASON_ID",
        "TEAM_NAME",
        "TEAM_ABBREVIATION",
        "MIN",
        "gameId",
        "teamId",
        "assists",
        "fieldGoalPercentage",
        "teamCity_x",
        "teamName_x",
        "teamTricode_x",
        "teamSlug_x",
        "minutes_y",
        "teamCity_y",
        "teamName_y",
        "teamTricode_y",
        "teamSlug_y",
        "usagePercentage", 
        "estimatedUsagePercentage" 
    ],
                                    axis=1)
    team_games = team_games.rename({
        "WL": "WINLOSE",
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
        "contestedFieldGoalsMade": "FGM_CONTESTED",
        "contestedFieldGoalsAttempted": "FGA_CONTESTED",
        "contestedFieldGoalPercentage": "FG_PCT_CONTESTED",
        "uncontestedFieldGoalsMade": "FGM_UNCONTESTED",
        "uncontestedFieldGoalsAttempted": "FGA_UNCONTESTED",
        "uncontestedFieldGoalsPercentage": "FG_PCT_UNCONTESTED",
        "defendedAtRimFieldGoalsMade": "FGM_DEFENDED_AT_RIM",
        "defendedAtRimFieldGoalsAttempted": "FGA_DEFENDED_AT_RIM",
        "defendedAtRimFieldGoalPercentage": "FG_PCT_DEFENDED_AT_RIM",
        "estimatedOffensiveRating": "EST_OFF_RATING", 
        "offensiveRating": "OFF_RATING", 
        "estimatedDefensiveRating": "EST_DEF_RATING", 
        "defensiveRating": "DEF_RATING", 
        "estimatedNetRating": "EST_NET_RATING", 
        "netRating": "NET_RATING", 
        "assistPercentage": "AST_PCT", 
        "assistToTurnover": "AST_TO_TOV", 
        "assistRatio": "AST_RATIO", 
        "offensiveReboundPercentage": "OREB_PCT", 
        "defensiveReboundPercentage": "DREB_PCT", 
        "reboundPercentage": "REB_PCT",
        "estimatedTeamTurnoverPercentage": "EST_TOV_PCT",
        "turnoverRatio": "TOV_RATIO", 
        "effectiveFieldGoalPercentage": "EFFECTIVE_FG_PCT", 
        "trueShootingPercentage": "TS_PCT", 
        "estimatedPace": "EST_PACE", 
        "pace": "PACE", 
        "pacePer40": "PACE_PER_40", 
        "possessions": "POSSESSIONS"
    },
                                      axis=1)
    # lower all the column names
    team_games.columns = map(str.lower, team_games.columns)
    if chunk_id is not None:
        output_path = os.path.join(save_dir, f"games_results_chunk_{chunk_id}.json")
        team_games.to_json(output_path, orient="records", indent=2)

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
        team_scoreboards(game_ids, chunk_id=chunk_id, save_dir=output_dir)
        mark_chunk_completed(checkpoint_file, chunk_id)
        print(f"Finished chunk {chunk_id}")
        break 

#############################################################################################################################

if __name__ == "__main__":
    main()

#############################################################################################################################