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

import os
import json
import pandas as pd

#############################################################################################################################

############################
###                      ###
###  CONCAT_JSON_TO_CSV  ###
###                      ###
############################

# take all the JSON chunks and creates one CSV file 

def concat_json_to_csv(input_folder, output_folder, output_filename):
    all_data = []

    for filename in os.listdir(input_folder):
        if filename.endswith(".json"):
            file_path = os.path.join(input_folder, filename)
            with open(file_path, 'r', encoding='utf-8') as f:
                try:
                    data = json.load(f)

                    if isinstance(data, list):
                        all_data.extend(data)
                    elif isinstance(data, dict):
                        all_data.append(data)
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON from {filename}: {e}")

    if all_data:
        df = pd.DataFrame(all_data)

        os.makedirs(output_folder, exist_ok=True)  # Ensure output folder exists
        output_path = os.path.join(output_folder, output_filename)
        df.to_csv(output_path, index=False)

        print(f"CSV saved to: {output_path}")
    else:
        print("No valid JSON data found to combine.")

#############################################################################################################################

#concat_json_to_csv("./data/raw/games", "./data/processed/games", "games.csv")
#concat_json_to_csv("./data/raw/team_scoreboard", "./data/processed/team_scoreboard", "team_scoreboards.csv")