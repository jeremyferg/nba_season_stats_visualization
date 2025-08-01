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

        print(f"CSV saved to: {output_path}")
    else:
        print("No valid JSON data found to combine.")

#############################################################################################################################