###########################
###########################
#####                 #####
#####   game_tables   #####
#####                 #####
###########################
###########################

# gets the games, arenas, and broadcasters tables

#############################################################################################################################

###################
###             ###
###  LIBRARIES  ###
###             ###
###################

import os
import pandas as pd

#############################################################################################################################

############################
###                      ###
###  COMBINE_ALL_CHUNKS  ###
###                      ###
############################

# Takes all the json file game information from data/processed and returns
# a single pandas dataframe with this information

def combine_all_chunks(input_dir="data/raw/games"):
    all_dfs = []

    for fname in sorted(os.listdir(input_dir)):
        # collect all the json files with this name pattern
        # then read and append the information
        if fname.startswith("games_results_chunk_") and fname.endswith(".json"):
            path = os.path.join(input_dir, fname)
            print(f"Loading {fname}...")
            df = pd.read_json(path, orient="records")
            all_dfs.append(df)

    combined_df = pd.concat(all_dfs, ignore_index=True)
    print(f"\nCombined {len(all_dfs)} chunks. Total rows: {len(combined_df)}")

    return combined_df
    
#############################################################################################################################

################
###          ###
###  ARENAS  ###
###          ###
################

# using the extract games from combine_all_chunks(), creates an Arena table
# table colums:
    ## ARENA_ID
    ## ARENA_NAME
    ## ARENA_CITY
    ## ARENA_STATE

def arenas():
    # get the correct columns from the extracted games
    arenas_df = games_extract_df[['arena_name',
                          'arena_city',
                          'arena_state']]

    # drop duplicates and add an ID column
    arenas_df = arenas_df.drop_duplicates()
    arenas_df = arenas_df.dropna(how='all')
    arenas_df = arenas_df.reset_index(drop=True)
    arenas_df.insert(0, "arena_id", arenas_df.index)
    
    return arenas_df

#############################################################################################################################

###############
###         ###
###  GAMES  ###
###         ###
###############

# using the extract games from combine_all_chunks(), creates an Games table
# table colums:
    ## GAME_ID
    ## GAME_DATE
    ## HOME_TEAM_ID
    ## VISITOR_TEAM_ID
    ## ARENA_ID
    ## NAT_BROADCASTER_ID
    ## HOME_BROADCASTER_ID
    ## AWAY_BROADCASTER_ID
    ## HOME_TEAM_PTS
    ## VISITOR_TEAM_PTS
    ## OUTCOME
    
def games():
    # merge the extracted games with the arenas table (to get ARENA_ID)
    new_games_df = pd.merge(games_extract_df, 
                        arenas_df, 
                        on=['arena_name',
                          'arena_city',
                          'arena_state'],
                        how="left")

    # create the columns of the games table
    new_games_df = new_games_df[[
                                'game_id',
                                'gane_date',
                                'home_team_id',
                                'visitor_team_id',
                                'arena_id',
                                'nat_broadcaster_id',
                                'home_broadcaster_id',
                                'visitor_broadcaster_id',
                                'home_team_pts',
                                'visitor_team__pts',
                                'outcome'
                                 ]]

    # change ID datatypes from floats to ints
    new_games_df['nat_broadcaster_id'] = new_games_df['nat_broadcaster_id'].astype("Int64")
    new_games_df['home_broadcaster_id'] = new_games_df['home_broadcaster_id'].astype("Int64")
    new_games_df['visitor_broadcaster_id'] = new_games_df['visitor_broadcaster_id'].astype("Int64")
    

    return new_games_df

#############################################################################################################################

######################
###                ###
###  BROADCASTERS  ###
###                ###
######################

# using the extract games from combine_all_chunks(), creates an Broadcaster table
# table colums:
    ## BROADCASTER_ID
    ## BROADCASTER_NAME

def broadcasters():
    # get the correct broadcaster columns and change there names to one convention
    nat_broadcaster= games_extract_df[[
        'nat_broadcaster_id',
        'nat_broadcaster_name'
    ]]
    home_broadcaster= games_extract_df[[
        'home_broadcaster_id',
        'home_broadcaster_name'
    ]]
    away_broadcaster= games_extract_df[[
        'visitor_broadcaster_id',
        'visitor_broadcaster_name'
    ]]
    for df in [nat_broadcaster, home_broadcaster, away_broadcaster]:
        df.columns = ["broadcaster_id", "broadcaster_name"]

    # concat these separte dataframes
    broadcaster_df = pd.concat([nat_broadcaster, home_broadcaster, away_broadcaster], ignore_index=True)

    # drop duplicates and NA values
    broadcaster_df = broadcaster_df.drop_duplicates()
    broadcaster_df = broadcaster_df.dropna()
    broadcaster_df = broadcaster_df.reset_index(drop=True)

    # change ID datatypes from floats to ints
    broadcaster_df['broadcaster_id'] = broadcaster_df['broadcaster_id'].astype(int)
    
    return broadcaster_df

#############################################################################################################################

##############
###        ###
###  MAIN  ###
###        ###
##############

def main():

    global games_extract_df, arenas_df

    games_extract_df = combine_all_chunks()
    print("Combined extracted games.")

    arenas_df = arenas()
    print("Built arena table.")
    arenas_df.to_csv("data/processed/games/arenas.csv", index=False)

    games_df = games()
    print("Built games table.")
    games_df.to_csv("data/processed/games/games.csv", index=False)

    broadcaster_df = broadcasters()
    print("Built broadcasters table.")
    broadcaster_df.to_csv("data/processed/games/broadcasters.csv", index=False)

#############################################################################################################################

if __name__ == "__main__":
    main()

#############################################################################################################################