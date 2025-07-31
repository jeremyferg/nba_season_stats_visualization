#############################
#############################
#####                   #####
#####   player_tables   #####
#####                   #####
#############################
#############################

# gets the player info, player seaosn, player career, awards, and player scoreboard tables

#############################################################################################################################

############################
###                      ###
###  LIBRARIES/DATASETS  ###
###                      ###
############################

import os
import json
import time
import numpy as np
import pandas as pd
from tqdm import tqdm
from nba_api.stats.endpoints import commonplayerinfo
from nba_api.stats.endpoints import playercareerstats
from nba_api.stats.endpoints import playerawards
from concat_chunks import concat_json_to_csv

output_path = "data/raw/player_scoreboard/player_ids.json"
# Read the JSON data
with open(output_path, 'r') as outfile:
    player_ids = json.load(outfile)

#############################################################################################################################

########################
###                  ###
###  FETCH_NBA_DATA  ###
###                  ###
########################

# calls and collects a specific endpoint/player in the API
# returns a dataframe of that API call

def fetch_nba_data(endpoint_class, player_id):
    response = endpoint_class(player_id=player_id)
    df = response.get_data_frames()[0]
    return df

#############################################################################################################################

#########################
###                   ###
###  PLAYERS_HELPERS  ###
###                   ###
#########################

# calls/collects all information for players and a specific endpoint method
# returns an aggerate dataframe of this information

def players_helper(method, active_player_id_list=[]):
    
    df = pd.DataFrame()
    failed = []

    for player_id in tqdm(active_player_id_list, desc="Fetching player career"):
        try:
            player_df = fetch_nba_data(method, player_id)
            df = pd.concat([df, player_df], ignore_index=True)

            time.sleep(.4)  # Pause for 1 second between requests

        except Exception as e:
            print(f"Failed to fetch Player ID {player_id}: {e}")
            failed.append(player_id)
            time.sleep(.4)  # Wait a bit longer after a failure

    for player_id in tqdm(failed, desc="Retrying failed games"):
        attempts = 0
        while attempts < 2:
            try:
                player_df = fetch_nba_data(method, player_id)
                df = pd.concat([df, player_df], ignore_index=True)
                time.sleep(.4)  # Pause for 1 second between requests
                break
            except Exception as e:
                print(f"Retry failed for Player Id: {player_id}, retrying in 3 seconds: {e}")
                attempts += 1
                time.sleep(.4)

    return df

#############################################################################################################################

######################
###                ###
###  PLAYERS_INFO  ###
###                ###
######################

# table columns:
    ## PLAYER_ID			## DRAFT_YEAR
    ## FIRST_NAME           ## DRAFT_ROUND
    ## LAST_NAME            ## DRAFT_NUM
    ## BIRTHDATE            ## NBA_75
    ## SCHOOL
    ## COUNTRY
    ## HEIGHT
    ## WEIGHT
    ## SEASON_EXP
    ## JERSEY
    ## POSITION
    ## TEAM_ID 
    ## FROM_YEAR
    ## TO_YEAR
    ## GLEAGUE

def player_info(player_ids):

    # get player info from the API
    player_info = players_helper(commonplayerinfo.CommonPlayerInfo, player_ids)

    # drop and rename columns
    player_info = player_info.drop([
        'GAMES_PLAYED_FLAG',
        'NBA_FLAG',
        'PLAYERCODE',
        'TEAM_CODE',
        'TEAM_NAME',
        'TEAM_ABBREVIATION',
        'TEAM_CITY',
        'GAMES_PLAYED_CURRENT_SEASON_FLAG',
        'ROSTERSTATUS',
        'LAST_AFFILIATION',
        'PLAYER_SLUG',
        'DISPLAY_FIRST_LAST',
        'DISPLAY_LAST_COMMA_FIRST',
        'DISPLAY_FI_LAST'
    ],
                    axis=1)
    player_info = player_info.rename({
        'PERSON_ID': 'PLAYER_ID',
        'DLEAGUE_FLAG': 'GLEAGUE',
        'DRAFT_NUMBER': 'DRAFT_NUM',
        'GREATEST_75_FLAG': 'NBA_75'
    },
                    axis=1)
    # change the formatting of birthdate
    player_info["BIRTHDATE"] = pd.to_datetime(player_info["BIRTHDATE"], errors='coerce')
    player_info['BIRTHDATE'] = player_info['BIRTHDATE'].dt.strftime('%m-%d-%Y')

    # lower all the column names
    player_info.columns = map(str.lower, player_info.columns)

    return player_info

#############################################################################################################################

###############################
###                         ###
###  PLAYERS_SEASON_CAREER  ###
###                         ###
###############################

# the seasons table gives basic season statistics for each player
# the career table groups a player's stats for each respective team they've been on

# table columns:
    ## PLAYER_ID                        ## FT_PCT
    ## SEASON_ID (only seasons)         ## OREB 
    ## TEAM_ID                          ## DREB
    ## GP                               ## REB
    ## GS                               ## AST
    ## NUM_YEARS (only careeres)        ## STL
    ## MINUTES                          ## BLK
    ## FGM                              ## TOV
    ## FGA                              ## PF
    ## FG_PCT                           ## PTS
    ## FG3M
    ## FG3A
    ## FG3_PCT
    ## FTM
    ## FTA

def player_season_career(player_ids):
    
    # get player careers from the API
    player_career = players_helper(playercareerstats.PlayerCareerStats, player_ids)

    # drop and rename columns
    player_career = player_career.drop([
        'LEAGUE_ID',
        'PLAYER_AGE',
        'TEAM_ABBREVIATION'
    ],
                    axis=1)
    player_career = player_career.rename({
        'MIN': 'MINUTES'
    },
                    axis=1)

    # copy a column for player seasons
    player_season = player_career.copy()

    # get rid of the columns where team id is 0 (those are trade columns)
    player_career = player_career[player_career['TEAM_ID'] != 0]

    # drop season id for the careers table
    player_career = player_career.drop([
        'SEASON_ID'
    ],
                    axis=1)
    # sum up stats for each team the player has played for
    player_career_agg = player_career.groupby(['PLAYER_ID', 'TEAM_ID'], as_index=False).agg({
    'TEAM_ABBREVIATION': 'first',
    'GP': 'sum',
    'GS': 'sum',
    'MIN': 'sum',
    'FGM': 'sum',
    'FGA': 'sum',
    'FG_PCT': 'mean',
    'FG3M': 'sum',
    'FG3A': 'sum',
    'FG3_PCT': 'mean',
    'FTM': 'sum',
    'FTA': 'sum',
    'FT_PCT': 'mean',
    'OREB': 'sum',
    'DREB': 'sum',
    'REB': 'sum',
    'AST': 'sum',
    'STL': 'sum',
    'BLK': 'sum',
    'TOV': 'sum',
    'PF': 'sum',
    'PTS': 'sum'      
}) 
    # create a column that counts the number of years a player has been on the team
    num_years = player_career.groupby(['PLAYER_ID', 'TEAM_ID']).size().reset_index(name="NUM_YEARS")

    # merge the counts into the aggregated table
    player_career = pd.merge(player_career_agg, num_years, on=["PLAYER_ID", "TEAM_ID"])
    cols = list(player_career.columns)
    cols.insert(5, cols.pop(cols.index('NUM_YEARS')))
    player_career = player_career[cols]

    # lower all the column names
    player_season.columns = map(str.lower, player_season.columns)
    player_career.columns = map(str.lower, player_career.columns)
                
    return player_season, player_career

#############################################################################################################################

################
###          ###
###  AWARDS  ###
###          ###
################

# table columns (player_awards):        table columns (awards):
    ## PLAYER_ID                            ## AWARD_ID                  
    ## AWARD_ID                             ## AWARD_NAME
    ## SEASON_ID
    ## MONTH
    ## WEEK

def awards(player_ids):

    # get the player awards info from the API
    player_awards = players_helper(playerawards.PlayerAwards, player_ids)

    # rename several values 
    conditions = [
        (player_awards["DESCRIPTION"]== "All-Rookie Team") & (player_awards["ALL_NBA_TEAM_NUMBER"]=='1'),
        (player_awards["DESCRIPTION"]== "All-Rookie Team") & (player_awards["ALL_NBA_TEAM_NUMBER"]=='2'),
        (player_awards["DESCRIPTION"]== "All-Defensive Team") & (player_awards["ALL_NBA_TEAM_NUMBER"]=='1'),
        (player_awards["DESCRIPTION"]== "All-Defensive Team") & (player_awards["ALL_NBA_TEAM_NUMBER"]=='2'),
        (player_awards["DESCRIPTION"]== "All-NBA") & (player_awards["ALL_NBA_TEAM_NUMBER"]=='1'),
        (player_awards["DESCRIPTION"]== "All-NBA") & (player_awards["ALL_NBA_TEAM_NUMBER"]=='2'),
        (player_awards["DESCRIPTION"]== "All-NBA") & (player_awards["ALL_NBA_TEAM_NUMBER"]=='3')
    ]
    choices = [
        "All-Rookie 1st Team",
        "All-Rookie 2nd Team",
        "All-Defensive 1st Team",
        "All-Defensive 2nd Team",
        "All-NBA 1st Team",
        "All-NBA 2nd Team",
        "All-NBA 3rd Team"
    ]
    player_awards["DESCRIPTION"] = np.select(conditions, choices, default=player_awards["DESCRIPTION"])
    # drop rows that are no longer relevant
    player_awards = player_awards[
        ~player_awards['DESCRIPTION'].isin([
            'NBA Sporting News Most Valuable Player of the Year',
            'NBA Sporting News Rookie of the Year',
            'Olympic Appearance'
        ])
    ]
    # drop irrelevant columns
    player_awards = player_awards.drop([
        'ALL_NBA_TEAM_NUMBER',
        'CONFERENCE',
        'TYPE',
        'SUBTYPE1',
        'SUBTYPE2',
        'SUBTYPE3'], 
                                       axis=1)
    player_awards = player_awards.rename({
        'PERSON_ID': 'PLAYER_ID',
        'SEASON': 'SEASON_ID'
    },
                    axis=1)

    # set week and month columns to appropriate values
    player_awards["WEEK"] = pd.to_datetime(player_awards["WEEK"], errors='coerce')
    player_awards["WEEK"] = player_awards["WEEK"].dt.date
    player_awards['WEEK'] = player_awards['WEEK'].apply(
        lambda x: x if pd.notnull(x) or x >= pd.to_datetime('2014-10-14') else None
    )
    player_awards["MONTH"] = pd.to_datetime(player_awards["MONTH"], errors="coerce").dt.month_name()
    player_awards['MONTH'] = player_awards['MONTH'].apply(lambda x: x if pd.notnull(x) else None)

    # get uniquee awards and rearrange awards values
    awards = player_awards["DESCRIPTION"].unique()
    awards = pd.DataFrame(awards, columns=['AWARD_NAME'])
    awards = awards.sort_values('AWARD_NAME')
    bronze = awards[awards['AWARD_NAME'] == 'Olympic Bronze Medal']
    awards = awards[awards['AWARD_NAME'] != 'Olympic Bronze Medal']
    awards = pd.concat([awards, bronze], ignore_index=True).reset_index(names='AWARD_ID')
    awards = awards[['AWARD_ID', 'AWARD_NAME']]

    # merge player_awards and awards so player_awards can have the IDs of awards
    player_awards = pd.merge(
                    player_awards, 
                    awards, 
                    left_on='DESCRIPTION',
                    right_on='AWARD_NAME',
                    how="left") 

    cols = list(player_awards.columns)
    cols.insert(5, cols.pop(cols.index('AWARD_ID')))
    player_awards = player_awards[cols]

    # drop descriptions, they are no longer needed 
    player_awards = player_awards.drop([
        'DESCRIPTION',
        'AWARD_NAME'

    ],
                                       axis=1)

    # lower all the column names
    player_awards.columns = map(str.lower, player_awards.columns)
    awards.columns = map(str.lower, awards.columns)

    return awards, player_awards

#############################################################################################################################

###########################
###                     ###
###  PLAYER_SCOREBOARD  ###
###                     ###
###########################

# table columns:
    ## PLAYER_ID           ## OREB                ## TOUCHES                    ## EST_DEF_RATING          ## PACE_PER_40
    ## GAME_ID             ## DREB                ## SECONDARY_AST              ## DEF_RATING              ## POSSESSIONS
    ## GAME_DATE           ## REB                 ## FT_AST                     ## EST_NET_RATING          ## PIE
    ## MATCHUP             ## AST                 ## PASSES                     ## NET_RATING
    ## WINLOSE             ## STL                 ## FGM_CONTESTED              ## AST_PCT
    ## PTS                 ## BLK                 ## FGA_CONTESTED              ## AST_TO_TOV
    ## FGM                 ## TOV                 ## FG_PCT_CONTESTED           ## AST_RATIO
    ## FGA                 ## PF                  ## FGM_UNCONTESTED            ## OREB_PCT
    ## FG_PCT              ## PLUS_MINUS          ## FGA_UNCONTESTED            ## DREB_PCT
    ## FG3M                ## MINUTES             ## FG_PCT_UNCONTESTED         ## REB_PCT
    ## FG3A                ## SPEED               ## FGM_DEFENDED_AT_RIM        ## TOV_RATIO
    ## FG3_PCT             ## DISTANCE            ## FGA_DEFENDED_AT_RIM        ## EFFECTIVE_FG_PCT
    ## FTM                 ## REB_CHANCES_OFF     ## FG_PCT_DEFENDED_AT_RIM     ## TS_PCT
    ## FTA                 ## REB_CHANCES_DEF     ## EST_OFF_RATING             ## EST_PACE
    ## FT_PCT              ## REB_CHANCES_TOTAL   ## OFF_RATING                 ## PACE

def player_scoreboard():

    concat_json_to_csv("./data/raw/player_scoreboard", "./data/processed/player_scoreboard", "player_scoreboards.csv")

#############################################################################################################################

##############
###        ###
###  MAIN  ###
###        ###
##############

def main():

    # split player ID list into three parts to avoid the API from completely timing out
    # script needs to be ran nine times to fully finish

    if not os.path.exists("data/processed/player_scoreboard/player_info_500.csv"):
        print("Building player info table (1-500).")
        player_info_df = player_info(player_ids[:500])       
        player_info_df.to_csv("data/processed/player_scoreboard/player_info_500.csv", index=False)

    elif not os.path.exists("data/processed/player_scoreboard/player_info_1000.csv"):
        print("Building player info table (501-1000).")
        player_info_df = player_info(player_ids[500:1000])      
        player_info_df.to_csv("data/processed/player_scoreboard/player_info_1000.csv", index=False)

    elif not os.path.exists("data/processed/player_scoreboard/player_info_1500.csv"):
        print("Building player info table (1001+).")
        player_info_df = player_info(player_ids[1000:])       
        player_info_df.to_csv("data/processed/player_scoreboard/player_info_1500.csv", index=False)

    elif not os.path.exists("data/processed/player_scoreboard/player_seasons_500.csv"):
        print("Builing player seasons/career tables (1-500).")
        player_season_df, player_career_df = player_season_career(player_ids[:500])
        player_season_df.to_csv("data/processed/player_scoreboard/player_seasons_500.csv", index=False)
        player_career_df.to_csv("data/processed/player_scoreboard/player_career_500.csv", index=False)

    elif not os.path.exists("data/processed/player_scoreboard/player_seasons_1000.csv"):
        print("Builing player seasons/career tables(501-1000).")
        player_season_df, player_career_df = player_season_career(player_ids[500:1000])
        player_season_df.to_csv("data/processed/player_scoreboard/player_seasons_1000.csv", index=False)
        player_career_df.to_csv("data/processed/player_scoreboard/player_career_1000.csv", index=False)
    
    elif not os.path.exists("data/processed/player_scoreboard/player_seasons_1500.csv"):
        print("Builing player seasons/career tables (1001+).")
        player_season_df, player_career_df = player_season_career(player_ids[1000:])
        player_season_df.to_csv("data/processed/player_scoreboard/player_seasons_1500.csv", index=False)
        player_career_df.to_csv("data/processed/player_scoreboard/player_career_1500.csv", index=False)

    elif not os.path.exists("data/processed/player_scoreboard/awards_500.csv"):
        print("Building awards tables (1-500).")
        awards_df, player_awards_df = awards(player_ids[:500])
        awards_df.to_csv("data/processed/player_scoreboard/awards_500.csv", index=False)
        player_awards_df.to_csv("data/processed/player_scoreboard/player_awards_500.csv", index=False)

    elif not os.path.exists("data/processed/player_scoreboard/awards_1000.csv"):
        print("Building awards tables (501-1000).")
        awards_df, player_awards_df = awards(player_ids[500:1000])
        awards_df.to_csv("data/processed/player_scoreboard/awards_1000.csv", index=False)
        player_awards_df.to_csv("data/processed/player_scoreboard/player_awards_1000.csv", index=False)
    
    elif not os.path.exists("data/processed/player_scoreboard/awards_1500.csv"):
        print("Building awards tables (1000+).")
        awards_df, player_awards_df = awards(player_ids[1000:])
        awards_df.to_csv("data/processed/player_scoreboard/awards_1500.csv", index=False)
        player_awards_df.to_csv("data/processed/player_scoreboard/player_awards_1500.csv", index=False)
    
    elif not os.path.exists("data/processed/player_scoreboard/player_scoreboards.csv"):
        player_scoreboard()

    else:
        print("All player information extracted.")

#############################################################################################################################

if __name__ == "__main__":
    main()

#############################################################################################################################