### TODO: redo in Snowflake

import pandas as pd
import numpy as np
import ast
from pathlib import Path


def filter_artists_without_ids(artists):
    artists = ast.literal_eval(artists)
    artists = [x for x in artists if (x['id'] is None) == False]
    return artists


venue_dfs = []
promoter_dfs = []
artist_dfs = []
event_table = []


for fname in Path("../../data/event_meta_data/").glob("*.csv"):
    lineup_as_aids = []
    country_name = str(fname).split('/')[-1].split('.csv')[0]
    country_df = pd.read_csv(fname).drop_duplicates()
    
    print(country_name)

    # create venue table
    venue_df = country_df[['venue_id', 'venue_name', 'venue_address', 'venue_area_id', 'venue_lat', 'venue_lng']].drop_duplicates()
    venue_dfs.append(venue_df)

    #"""
    # create promoter table 
    promoter_df = country_df[['promoter_id', 'promoter_name']].drop_duplicates()
    promoter_dfs.append(promoter_df)

    # create artist dict
    aid_2_name = {}

    lineups_all = country_df.lineup.values
    for artists in lineups_all:
        artists = filter_artists_without_ids(artists)

        # i.e. if we have at least some artists
        if len(artists) > 0:
                event_lineup = [aid_x_aname['id'] for aid_x_aname in artists] # as ids
                event_lineup = ','.join(str(x) for x in event_lineup)
                for aid_x_aname in artists:
                    aid_2_name[aid_x_aname['id']] = aid_x_aname['name']
        else:
            event_lineup = np.nan
        lineup_as_aids.append(event_lineup)
                    

    artist_df = pd.DataFrame(aid_2_name.items(), columns=['artist_id', 'artist_name'])
    artist_dfs.append(artist_df)
    country_df['linup_aids'] = lineup_as_aids

    # finally, remove redunant cols in original table
    country_df = country_df[['id', 'title', 'description', 'start_time', 'lineup', 'linup_aids', 'genres', 'interested_count', 'is_festival', 'has_secret_venue', 'is_ticketed', 'flyer_photo', 'venue_id', 'promoter_id']]
    event_table.append(country_df)
    

venue_dfs = pd.concat(venue_dfs).drop_duplicates()
promoter_dfs = pd.concat(promoter_dfs).drop_duplicates()
artist_dfs = pd.concat(artist_dfs).drop_duplicates()
event_table = pd.concat(event_table).drop_duplicates()

venue_dfs = venue_dfs.reset_index()
promoter_dfs = promoter_dfs.reset_index()
artist_dfs = artist_dfs.reset_index()
event_table = event_table.reset_index()

venue_dfs.to_csv('../../data/clean_tables/venues')
promoter_dfs.to_csv("../../data/clean_tables/promoters")
artist_dfs.to_csv("../../data/clean_tables/artists")
event_table.to_csv("../../data/clean_tables/events")
