###########################
###########################
#####                 #####
#####   team_tables   #####
#####                 #####
###########################
###########################

# gets the team info, team seasons, lineups, and team scoreboard tables

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
import numpy as np
from tqdm import tqdm
from nba_api.stats.static import teams
from nba_api.stats.endpoints import franchisehistory, leaguedashteamstats, leaguedashlineups, leaguelineupviz
from concat_chunks import concat_json_to_csv

output_path = "data/raw/team_scoreboard/team_ids.json"
# Read the JSON data
with open(output_path, 'r') as outfile:
    team_ids_list = json.load(outfile)

#############################################################################################################################

###################
###             ###
###  TEAM_INFO  ###
###             ###
###################

# collects and creates a Team Info table
# table colums:
    ## TEAM_ID	
    ## TEAM_CITY	
    ## TEAM_NAME	
    ## START_YEAR	
    ## END_YEAR	
    ## GAMES	
    ## WINS	
    ## LOSSES	
    ## WIN_PCT	
    ## PO_APPEARANCES	
    ## DIV_TITLES	
    ## CONF_TITLES	
    ## LEAGUE_TITLES

def team_info():

    # get basic information on nba teams
    nba_teams = teams.get_teams()
    nba_teams_df = pd.DataFrame(nba_teams)

    # get historic information on nba teams
    franchise_teams = franchisehistory.FranchiseHistory().get_data_frames()[0]
    franchise_teams["START_YEAR"] = franchise_teams["START_YEAR"].astype('int64')
    franchise_teams['TEAM_CITY'] = np.where(franchise_teams['TEAM_CITY'] == 'LA', 'Los Angeles', franchise_teams['TEAM_CITY'])
    franchise_teams = franchise_teams[franchise_teams["END_YEAR"] == '2024']


    # merge the two dataframes together
    team_info = pd.merge(nba_teams_df, 
                         franchise_teams, 
                         left_on=["id", "year_founded", "city"],
                         right_on=["TEAM_ID", "START_YEAR", "TEAM_CITY"],
                         how="inner")
    team_info = team_info.iloc[:-1]

    # drop the unnecessary columns
    team_info = team_info.drop([
    'id',
    'full_name',
    'abbreviation',
    'nickname',
    'city',
    'state',
    'year_founded',
    'LEAGUE_ID'
    ], axis=1)

    tot_row_df = pd.DataFrame([{
        'TEAM_ID': 0, 
        'TEAM_CITY': None,
        'TEAM_NAME': 'Trade',
        'START_YEAR': None,
        'END_YEAR': None,
        'YEARS': None,
        'GAMES': None,
        'WINS': None,
        'LOSSES': None,
        'WIN_PCT': None,
        'PO_APPEARANCES': None,
        'DIV_TITLES': None,
        'CONF_TITLES': None,
        'LEAGUE_TITLES': None}])

    team_info = pd.concat([team_info, tot_row_df], ignore_index=True)
    team_info.columns = map(str.lower, team_info.columns)

    return team_info

#############################################################################################################################

######################
###                ###
###  TEAM_SEASONS  ###
###                ###
######################

# collects and creates a Team Seasons table
# table colums:
    ## TEAM_ID			## FTA																							
    ## TEAM_NAME	    ## FT_PCT
    ## SEASON	        ## OREB
    ## GP	            ## DREB
    ## W		        ## REB
    ## L	            ## AST
    ## W_PCT	        ## TOV
    ## MIN	            ## STL
    ## FGM	            ## BLK
    ## FGA	            ## BLKA
    ## FG_PCT	        ## PF
    ## FG3M	            ## PFD
    ## FG3A             ## PTS
    ## FG3_PCT          ## PLUS_MINUS
    ## FTM

def team_seasons():

    # get the seasons we would like to extract
    seasons = list(range(2014,2025))

    df = pd.DataFrame()

    # extract season data
    for season in tqdm(seasons, desc="Fetching team career"):
        try:
            season_df = leaguedashteamstats.LeagueDashTeamStats(
                league_id_nullable='00', 
                date_from_nullable=f"{season}-10-10", 
                season=f"{season}-{season-1999}").get_data_frames()[0]
            season_df.insert(2, 'SEASON', f'{season}-{season+1}')
            df = pd.concat([df, season_df], ignore_index=True)

            time.sleep(.4)

        except Exception as e:
            print(f"Failed to fetch Year {season}: {e}")

            time.sleep(.4)

    # drop columns, sort by team and season
    df = df.drop(columns=[col for col in df.columns if col.endswith('_RANK')])
    df = df.sort_values(by=['TEAM_ID', 'SEASON'], ignore_index=True)
    df.columns = map(str.lower,df.columns)

    return df

#############################################################################################################################

#################
###           ###
###  LINEUPS  ###
###           ###
#################

# collects and creates a Lineups table
# table colums:
    ## GROUP_ID			## FGA          ##	BLKA            ##	PCT_PTS_FB																																							
    ## GROUP_NAME	    ## FG_PCT       ##	PF              ##	PCT_PTS_FT
    ## PLAYER_0	        ## FG3M         ##	PFD             ##	PCT_PTS_PAINT
    ## PLAYER_1	        ## FG3A         ##	PTS             ##	PCT_AST_FGM
    ## PLAYER_2		    ## FG3_PCT      ##	PLUS_MINUS      ##	PCT_UAST_FGM
    ## PLAYER_3	        ## FTM          ##	OFF_RATING      ##	OPP_FG3_PCT
    ## PLAYER_4	        ## FTA          ##	DEF_RATING      ##	OPP_FTA_RATE
    ## TEAM_ID	        ## FT_PCT       ##	NET_RATING      ##	OPP_TOV_PCT
    ## GP	            ## OREB         ##	PACE
    ## W	            ## DREB         ##	TS_PCT        
    ## L	            ## REB          ##	FTA_RATE
    ## W_PCT	        ## AST          ##	TEAM_AST_PCT
    ## MIN              ## TOV          ##	PCT_FGA_2PT
    ## FG3_PCT          ## STL          ##	PCT_FGA_3PT
    ## FGM              ## BLK          ##	PCT_PTS_MR

def lineups(teams_id):

    # get the seasons we would like to extract
    seasons=['2014-15', '2015-16', '2016-17', '2017-18', '2018-19',
             '2019-20', '2020-21', '2021-22', '2022-23', '2023-24', '2024-25']

    df = pd.DataFrame()
    failed = []

    # get all game lineup information
    for team_id in tqdm(teams_id, desc="Fetching team lineups"):
        for season in tqdm(seasons, desc="Fetching seasons"):
            try:
                lineup_basic = leaguedashlineups.LeagueDashLineups(team_id_nullable=team_id, season=season).get_data_frames()[0]
                time.sleep(.4)
                lineup_basic = lineup_basic.drop(columns=[col for col in lineup_basic.columns if col.endswith('_RANK')], axis=1)
                lineup_basic = lineup_basic.drop([
                    'GROUP_SET',
                    'TEAM_ABBREVIATION'
                ], axis=1)

                split_cols = lineup_basic["GROUP_ID"].str.findall(r'\d+')
                split_df = pd.DataFrame(split_cols.tolist(), columns=[f"PLAYER_{i}" for i in range(split_cols.str.len().max())+1])
                lineup_basic = pd.concat([lineup_basic.iloc[:, :2], split_df, lineup_basic.iloc[:, 2:]], axis=1)
            
                
                lineup_advanced = leaguelineupviz.LeagueLineupViz(team_id_nullable=team_id, season=season, minutes_min=0).get_data_frames()[0]
                time.sleep(.4)  # Pause for 1 second between requests
                lineup_advanced = lineup_advanced.drop([
                    'TEAM_ID',
                    'TEAM_ABBREVIATION',
                    'MIN'
                ], axis=1)
                lineup_advanced = lineup_advanced.rename({
                    "TM_AST_PCT": "TEAM_AST_PCT",
                    "PCT_PTS_2PT_MR": "PCT_PTS_MR"
                }, axis=1)

                lineup_merged = pd.merge(
                    lineup_basic, 
                    lineup_advanced, 
                    on=['GROUP_ID', 'GROUP_NAME'],
                    how="left")

                df = pd.concat([df, lineup_merged], ignore_index=True)

            except Exception as e:
                print(f"Failed to fetch team {team_id}: {e}")
                failed.append([team_id, season])
                time.sleep(.4)  # Wait a bit longer after a failure

    for grouping in tqdm(failed, desc="Fetching seasons"):
        attempts = 0
        while attempts < 2:
            try:
                lineup_basic = leaguedashlineups.LeagueDashLineups(team_id_nullable=grouping[0], season=grouping[1]).get_data_frames()[0]
                time.sleep(.4)
                lineup_basic = lineup_basic.drop(columns=[col for col in lineup_basic.columns if col.endswith('_RANK')], axis=1)
                lineup_basic = lineup_basic.drop([
                    'GROUP_SET',
                    'TEAM_ABBREVIATION'
                ], axis=1)

                split_cols = lineup_basic["GROUP_ID"].str.findall(r'\d+')
                split_df = pd.DataFrame(split_cols.tolist(), columns=[f"PLAYER_{i}" for i in range(split_cols.str.len().max())])
                lineup_basic = pd.concat([lineup_basic.iloc[:, :2], split_df, lineup_basic.iloc[:, 2:]], axis=1)
        
            
                lineup_advanced = leaguelineupviz.LeagueLineupViz(team_id_nullable=grouping[0], season=grouping[1], minutes_min=0).get_data_frames()[0]
                time.sleep(.4)  # Pause for 1 second between requests
                lineup_advanced = lineup_advanced.drop([
                    'TEAM_ID',
                    'TEAM_ABBREVIATION',
                    'MIN'
                ], axis=1)

                lineup_merged = pd.merge(
                    lineup_basic, 
                    lineup_advanced, 
                    on=['GROUP_ID', 'GROUP_NAME'],
                    how="left")

                df = pd.concat([df, lineup_merged], ignore_index=True)

            except Exception as e:
                print(f"Failed to fetch team {team_id}: {e}")
                attempts += 1
                time.sleep(.4)  # Wait a bit longer after a failure

    # make a proper id column for groups
    df['GROUP_ID'] = pd.factorize(df['GROUP_ID'])[0]

    # lower all the column names
    df.columns = map(str.lower, df.columns)

    return df

#############################################################################################################################

#########################
###                   ###
###  TEAM_SCOREBOARD  ###
###                   ###
#########################

# table columns:
    ## TEAM_ID             ## OREB                ## SECONDARY_AST                  ## DEF_RATING          ## POSSESSIONS
    ## GAME_ID             ## DREB                ## FT_AST                         ## EST_NET_RATING      ## PIE
    ## GAME_DATE           ## REB                 ## PASSES                         ## NET_RATING          
    ## MATCHUP             ## AST                 ## FGM_CONTESTED                  ## AST_PCT
    ## WINLOSE             ## STL                 ## FGA_CONTESTED                  ## AST_TO_TOV
    ## PTS                 ## BLK                 ## FG_PCT_CONTESTED               ## AST_RATIO
    ## FGM                 ## TOV                 ## FGM_UNCONTESTED                ## OREB_PCT
    ## FGA                 ## PF                  ## FGA_UNCONTESTED                ## DREB_PCT
    ## FG_PCT              ## PLUS_MINUS          ## FG_PCT_UNCONTESTED             ## REB_PCT
    ## FG3M                ## MINUTES             ## FGM_DEFENDED_AT_RIM            ## TOV_RATIO
    ## FG3A                ## DISTANCE            ## FGA_DEFENDED_AT_RIM            ## EFFECTIVE_FG_PCT
    ## FG3_PCT             ## REB_CHANCES_OFF     ## FG_PCT_DEFENDED_AT_RIM         ## TS_PCT
    ## FTM                 ## REB_CHANCES_DEF     ## EST_OFF_RATING                 ## EST_PACE
    ## FTA                 ## REB_CHANCES_TOTAL   ## OFF_RATING                     ## PACE
    ## FT_PCT              ## TOUCHES             ## EST_DEF_RATING                 ## PACE_PER_40

def team_scoreboard():

    concat_json_to_csv("./data/raw/team_scoreboard", "./data/processed/team_scoreboard", "team_scoreboards.csv")

#############################################################################################################################

##############
###        ###
###  MAIN  ###
###        ###
##############

# script needs to be ran four times to fully finish

def main():

    if not os.path.exists("data/processed/team_scoreboard/team_info.csv"):
        print("Building team info table.")
        team_info_df = team_info()
        team_info_df.to_csv("data/processed/team_scoreboard/team_info.csv", index=False)

    elif not os.path.exists("data/processed/team_scoreboard/team_seasons.csv"):
        print("Building team seasons table.")
        team_seasons_df = team_seasons()
        team_seasons_df.to_csv("data/processed/team_scoreboard/team_seasons.csv", index=False)

    else:
        if not os.path.exists("data/processed/team_scoreboard/lineups.csv"):
            print("Buildng lineups table (teams 1-15).")
            lineups_df = lineups(teams_id=team_ids_list[:15])
            lineups_df.to_csv("data/processed/team_scoreboard/lineups.csv", index=False)
        else:
            print("Buildng lineups table (teams 16-30).")
            lineups_df_2 = lineups(teams_id=team_ids_list[15:])
            lineups_df = pd.read_csv("data/processed/team_scoreboard/lineups.csv")
            lineups_df = pd.concat([lineups_df, lineups_df_2], ignore_index=True)
            lineups_df.to_csv("data/processed/team_scorebord/lineups.csv", index=False)

            print("All team information extracted.")
        
#############################################################################################################################
        
if __name__ == "__main__":
    main()

#############################################################################################################################