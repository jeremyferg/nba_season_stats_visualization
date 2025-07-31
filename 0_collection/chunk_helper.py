############################
############################
#####                  #####
#####   chunk_helper   #####
#####                  #####
############################
############################

# helper functions used to record which pieces of raw information have been processed
# functioned produced by Chat GPT

#############################################################################################################################

###################
###             ###
###  LIBRARIES  ###
###             ###
###################

import os
import json

#############################################################################################################################

##############################
###                        ###
###  GET_COMPLETED_CHUNKS  ###
###                        ###
##############################

# helper function that returns a list of extracted chunks

def get_completed_chunks(checkpoint_file):
    if not os.path.exists(checkpoint_file):
        return set()
    with open(checkpoint_file) as f:
        return set(int(line.strip()) for line in f if line.strip())

#############################################################################################################################

##############################
###                        ###
###  MARK_CHUNK_COMPLETED  ###
###                        ###
##############################

# helper function that marks a chunk as complete

def mark_chunk_completed(checkpoint_file, chunk_id):
    with open(checkpoint_file, "a") as f:
        f.write(f"{chunk_id}\n")

#############################################################################################################################

####################
###              ###
###  LOAD_CHUNK  ###
###              ###
####################

# helper function that loads the correct JSON file based on the required chunk

def load_chunk(chunk_dir, chunk_id):
    with open(os.path.join(chunk_dir, f"games_chunk_{chunk_id}.json"), "r") as f:
        return json.load(f)

#############################################################################################################################