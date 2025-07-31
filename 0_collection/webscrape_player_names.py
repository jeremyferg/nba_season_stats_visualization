######################################
######################################
#####                            #####
#####   webscrape_player_names   #####
#####                            #####
######################################
######################################

# this script web scrapes BasketballReference for unique player names from the 2014-15 season to the 2024-25 season
# returns a text file of unique names placed in data/raw

# code derived from https://medium.com/analytics-vidhya/intro-to-scraping-basketball-reference-data-8adcaa79664a

#############################################################################################################################

###################
###             ###
###  LIBRARIES  ###
###             ###
###################

from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np

#############################################################################################################################

print(f"Starting player extraction from BasketballReference.")

all_data = []

for year in range(2014, 2025):  # from 2014 to 2024 inclusive
    url = f"https://www.basketball-reference.com/leagues/NBA_{year}_totals.html"
    print(f"Scraping data for {year}...")

    try:
        html = urlopen(url)
        soup = BeautifulSoup(html, features="lxml")

        headers = [th.get_text() for th in soup.find_all('tr', limit=2)[0].find_all('th')]
        rows = soup.find_all('tr')[1:-1]

        rows_data = [[td.get_text() for td in row.find_all('td')] for row in rows if row.find_all('td')]
        
        last_year = 1
        for i in range(0, len(rows_data)):
            rows_data[i].insert(0, last_year)
            last_year +=1

        all_data.extend(rows_data)
    
    except Exception as e:
        print(f"Failed to scrape {year}: {e}")

# Create the full DataFrame
all_df = pd.DataFrame(all_data, columns=headers)

# Several player names have discrepancies between NBA API and BasketballReference
# the next group of code resolves this issuse
conditions = [
    all_df['Player'] == 'J.R. Smith',
    all_df['Player'] == 'J.J. Hickson',
    all_df['Player'] == 'C.J. Miles',
    all_df['Player'] == 'Vítor Luiz Faverani',
    all_df['Player'] == 'A.J. Price',
    all_df['Player'] == 'Glen Rice Jr.',
    all_df['Player'] == 'D.J. Stephens',
    all_df['Player'] == "Hamady N'Diaye",
    all_df['Player'] == 'D.J. White',
    all_df['Player'] == 'K.J. McDaniels',
    all_df['Player'] == 'P.J. Hairston',
    all_df['Player'] == 'R.J. Hunter',
    all_df['Player'] == "J.J. O'Brien",
    all_df['Player'] == 'Tibor Pleiß',
    all_df['Player'] == 'A.J. Hammons',
    all_df['Player'] == 'B.J. Johnson',
    all_df['Player'] == "Xavier Tillman Sr.",
    all_df['Player'] == 'Brandon Boston Jr.',
    all_df['Player'] == 'M.J. Walker',
    all_df['Player'] == 'A.J. Green',
    all_df['Player'] == 'GG Jackson II'
]
choices = [
    'JR Smith',
    'JJ Hickson',
    'CJ Miles',
    'Vítor Faverani',
    'AJ Price',
    'Glen Rice',
    'DJ Stephens',
    "Hamady NDiaye",
    'DJ White',
    'KJ McDaniels',
    'PJ Hairston',
    'RJ Hunter',
    "JJ O'Brien",
    'Tibor Pleiss',
    'AJ Hammons',
    'BJ Johnson',
    "Xavier Tillman",
    'Brandon Boston',
    'MJ Walker',
    'AJ Green',
    'GG Jackson'
]
all_df['Player'] = np.select(conditions, choices, default=all_df['Player'])

# Save full dataset to CSV
all_df.to_csv("nba_totals_2014_2024.csv", index=False)

# Extract unique player names from the "Player" column
unique_players = all_df["Player"].unique().tolist()

output_path = "data/raw/unique_players_2014_2024.txt"
# Save player names to a file
with open(output_path, "w", encoding="utf-8") as f:
    for name in unique_players:
        f.write(name + "\n")

print(f"Complete. Found {len(unique_players)} unique players.")

#############################################################################################################################