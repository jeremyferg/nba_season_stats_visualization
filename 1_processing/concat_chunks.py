#############################
#############################
#####                   #####
#####   concat_chunks   #####
#####                   #####
#############################
#############################

# helper functions used to concat games and scoreboard chunks into a single dataframe
# functioned produced by Chat GPT

#############################################################################################################################

###################
###             ###
###  LIBRARIES  ###
###             ###
###################

import json
import pandas as pd
from pathlib import Path

#############################################################################################################################

#####################
###               ###
###  FIX_MINUTES  ###
###               ###
#####################

# helper function that revises data entries in the CSV file

def fix_minutes(val):
    if isinstance(val, str) and ':' in val:
        parts = val.split(':')
        try:
            minutes = int(parts[0]) // 60
            seconds = int(parts[0]) % 60
            return f"{minutes}:{seconds:02d}"
        except:
            return None
    return val

#############################################################################################################################

############################
###                      ###
###  CONCAT_JSON_TO_CSV  ###
###                      ###
############################

# take all the JSON chunks and creates one CSV file 

def concat_json_to_csv(input_folder_rel, output_folder_rel, output_filename):
    # Dynamically resolve project root (one level up from script directory)
    project_root = Path(__file__).resolve().parent.parent

    # Resolve full paths from relative ones
    input_folder = project_root / input_folder_rel
    output_folder = project_root / output_folder_rel

    all_data = []

    for filename in input_folder.iterdir():
        if filename.suffix == ".json":
            with filename.open('r', encoding='utf-8') as f:
                try:
                    data = json.load(f)

                    if isinstance(data, list):
                        all_data.extend([item for item in data if isinstance(item, dict)])
                    elif isinstance(data, dict):
                        all_data.append(data)
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON from {filename.name}: {e}")

    if all_data:
        df = pd.DataFrame(all_data)

        output_folder.mkdir(parents=True, exist_ok=True)
        output_path = output_folder / output_filename
        df.to_csv(output_path, index=False)

        # clean up the columns depending on if we're cleaning team/player scoreboards
        if output_filename=="team_scoreboards.csv":
            df = df.drop([
        "SEASON_ID",
        "TEAM_ABBREVIATION",
        "USE_PCT",
        "EST_USE_PCT",
        "PIE",
        "MINUTES"
    ],
                                    axis=1)
            df = df.rename({
        "WL": "WINLOSE",
        "MIN": "MINUTES",
        "CONTESTED_FGM": "FGM_CONTESTED",
        "CONTESTED_FGA": "FGA_CONTESTED",
        "CONTESTED_FG_PCT": "FG_PCT_CONTESTED",
        "UNCONTESTED_FGM": "FGM_UNCONTESTED",
        "UNCONTESTED_FGA": "FGA_UNCONTESTED",
        "UNCONTESTED_FG_PCT": "FG_PCT_UNCONTESTED",
        "DEFENDED_AT_RIM_FGM": "FGM_DEFENDED_AT_RIM",
        "DEFENDED_AT_RIM_FGA": "FGA_DEFENDED_AT_RIM",
        "DEFENDED_AT_FG_PCT": "FG_PCT_DEFENDED_AT_RIM",
        "OFF_REB_PCT": "OREB_PCT", 
        "DEF_REB_PCT": "DREB_PCT", 
        "EST_TEAM_TOV_PCT": "EST_TOV_PCT"
    },
                                      axis=1)
            df.columns = map(str.lower, df.columns)
            df['minutes'] = df['minutes'].apply(fix_minutes)
            df = df.drop_duplicates(subset=['team_id', 'game_id'])
        else:
            uppercase_cols = [col for col in df.columns if col.isupper()]
            # Fully lowercase columns (letters must be lowercase, digits allowed)
            lowercase_cols = [col for col in player_scoreboards.columns if col.islower()]
            # Split the DataFrame
            df_upper = player_scoreboards[uppercase_cols]
            df_lower = player_scoreboards[lowercase_cols]
            df_upper = df_upper.dropna(how="all")
            df_lower = df_lower.dropna(how="all")
            columns_to_update = [
                ('fgm_contested', 'contested_fgm'),
                ('fga_contested', 'contested_fga'),
                ('fg_pct_contested', 'contested_fg_pct'),
                ('fgm_uncontested', 'uncontested_fgm'),
                ('fga_uncontested', 'uncontested_fga'),
                ('fg_pct_uncontested', 'uncontested_fg_pct'),
                ('fgm_defended_at_rim', 'defended_at_rim_fgm'),
                ('fga_defended_at_rim', 'defended_at_rim_fga'),
                ('fg_pct_defended_at_rim', 'defended_at_fg_pct'),
                ('oreb_pct', 'off_reb_pct'),
                ('dreb_pct', 'def_reb_pct')
            ]
            for source_col, target_col in columns_to_update:
                df_lower.loc[~df_lower[source_col].isna(), target_col] = df_lower[source_col]
            df_lower = df_lower.drop([
                'fgm_contested',
                'fga_contested',
                'fg_pct_contested',
                'fgm_uncontested',
                'fga_uncontested',
                'fg_pct_uncontested',
                'fgm_defended_at_rim',
                'fga_defended_at_rim',
                'fg_pct_defended_at_rim',
                'oreb_pct',
                'dreb_pct'
            ], axis=1)
            df_lower = df_lower.rename({
                'contested_fgm':'fgm_contested',
                'contested_fga':'fga_contested',
                'contested_fg_pct':'fg_pct_contested',
                'uncontested_fgm':'fgm_uncontested',
                'uncontested_fga':'fga_uncontested',
                'uncontested_fg_pct':'fg_pct_uncontested',
                'defended_at_rim_fgm':'fgm_defended_at_rim',
                'defended_at_rim_fga':'fga_defended_at_rim',
                'defended_at_fg_pct':'fg_pct_defended_at_rim',
                'off_reb_pct':'oreb_pct',
                'def_reb_pct':'dreb_pct'
            }, axis=1)
            df_upper = df_upper.drop([
                'SEASON_ID',
                'TEAM_ID',
                'TEAM_ABBREVIATION',
                'USE_PCT',
                'EST_USE_PCT'
            ], axis=1)
            df_upper = df_upper.rename({
                'WL': 'WINLOSE',
                "CONTESTED_FGM": "FGM_CONTESTED",
                "CONTESTED_FGA": "FGA_CONTESTED",
                "CONTESTED_FG_PCT": "FG_PCT_CONTESTED",
                "UNCONTESTED_FGM": "FGM_UNCONTESTED",
                "UNCONTESTED_FGA": "FGA_UNCONTESTED",
                "UNCONTESTED_FG_PCT": "FG_PCT_UNCONTESTED",
                "DEFENDED_AT_RIM_FGM": "FGM_DEFENDED_AT_RIM",
                "DEFENDED_AT_RIM_FGA": "FGA_DEFENDED_AT_RIM",
                "DEFENDED_AT_FG_PCT": "FG_PCT_DEFENDED_AT_RIM",
                "OFF_REB_PCT": "OREB_PCT", 
                "DEF_REB_PCT": "DREB_PCT", 
                "EST_TEAM_TOV_PCT": "EST_TOV_PCT"
            }, axis=1)
            df_upper.columns = map(str.lower, df_upper.columns)
            player_scoreboards = pd.concat([df_upper, df_lower], ignore_index=True)
            player_scoreboards['minutes'] = player_scoreboards['minutes'].apply(fix_minutes)

        print(f"CSV saved to: {output_path}")
    else:
        print("No valid JSON data found to combine.")

#############################################################################################################################