########################
########################
#####              #####
#####   main_0_1   #####
#####              #####
########################
########################

#############################################################################################################################

###################
###             ###
###  LIBRARIES  ###
###             ###
###################

import os
import time

log_file = "main_0_1_runtime_log.txt"

#############################################################################################################################

########################
###                  ###
###  LOG_CHECKPOINT  ###
###                  ###
########################

# used to record the times of each iteration and the script as a whole

def log_checkpoint(message):
    current_time = time.strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, "a") as f:
        f.write(f"[{current_time}] {message}\n")

#############################################################################################################################

##############
###        ###
###  MAIN  ###
###        ###
##############

def main():

    start_time = time.time()
    log_checkpoint("Script started")

    print("Phase 0: Data Collection")

    os.system("python 0_collection/0_webscrape_player_names.py")
    log_checkpoint("0_webscrape_player_names.py completed.")
    time.sleep(2)
    os.system("python 0_collection/1_get_id_lists.py")
    log_checkpoint("1_get_id_lists.py completed.")
    time.sleep(2)

    for i in range(3):
        os.system("python 0_collection/2_get_raw_player_scoreboards.py")
        log_checkpoint("2_get_raw_player_scoreboards.py completed.")

    for i in range(61):   
        print(f"Run {i+1}")
        log_checkpoint(f"Run {i+1} start.")

        os.system("python 0_collection/3_get_raw_detailed_player_scoreboards.py")
        log_checkpoint(f"3_get_raw_detailed_player_scoreboards.py completed.")
        time.sleep(2) 

        os.system("python 0_collection/4_get_raw_games.py")
        log_checkpoint(f"4_get_raw_games.py completed.")
        time.sleep(2)

        os.system("python 0_collection/5_get_raw_team_scoreboards.py")
        log_checkpoint(f"5_get_raw_team_scoreboards.py completed.")

    print("Phase 0 done.\nPhase 1: Data Processing")

    for i in range(9):
        print(f"Run {i+1}")
        log_checkpoint(f"Run {i+1} start.")

        os.system("python 1_processing/0_player_tables.py")
        log_checkpoint(f"0_player_tables.py completed.")
        time.sleep(2)

    for i in range(4):
        print(f"Run {i+1}")
        log_checkpoint(f"Run {i+1} start.")

        os.system("python 1_processing/0_team_tables.py")
        log_checkpoint(f"0_team_tables.py completed.")
        time.sleep(2)

    os.system("python 1_processing/0_game_tables.py")
    log_checkpoint(f"0_game_tables.py completed.")

    print("Phase 1 done.")

    end_time = time.time()
    runtime = end_time - start_time
    log_checkpoint(f"Script ended - Total runtime: {runtime:.2f} seconds")

#############################################################################################################################