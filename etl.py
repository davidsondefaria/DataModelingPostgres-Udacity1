import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """ Get song and artist information from song_dataset and insert into song_table and artist_table.
    
    Parameters:
    Argument1: Connect cursor to connect to database
    Argument2: path of dataset
    
    """
    # open song file
    df = pd.read_json(filepath, lines=True)
    
    # insert song record
    song_data = df[['song_id','title', 'artist_id', 'year', 'duration']]
    song_data = song_data.drop_duplicates()

    for i, row in song_data.iterrows():
        cur.execute(song_table_insert, row)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    artist_data = artist_data.drop_duplicates()

    for i, row in artist_data.iterrows():
        cur.execute(artist_table_insert, row)


def process_log_file(cur, filepath):
    """ Get time and and information from log_dataset and insert into time_table and user_table.
    Get songs from a specific page of log_dataset, match song_id and artist_id and insert into songplay_table.
    
    Parameters:
    Argument1: Connect cursor to connect to database
    Argument2: path of dataset
    
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    dropIndex = []
    for i, row in df.iterrows():
        if(row['page'] != 'NextSong'):
            dropIndex.append(i)
    df.drop(dropIndex, inplace=True)
    df.drop_duplicates()
    
    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = [list(x.time() for x in t),
                 list(x.hour for x in t),
                 list(x.day for x in t),
                 list(x.weekofyear for x in t),
                 list(x.month for x in t),
                 list(x.year for x in t),
                 list(x.weekday() for x in t)
                ]
    
    column_labels = ["start_time", "hour", "day", "week", "month", "year", "weekday"]
    
    time_dict = dict(zip(column_labels, time_data))
    
    time_df = pd.DataFrame.from_dict(time_dict)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = pd.DataFrame({'user_id': list(df['userId']),
                            'first_name': list(df['firstName']),
                            'last_name': list(df['lastName']),
                            'gender': list(df['gender']),
                            'level': list(df['level'])
                           })

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            print(results, row.song, row.artist, row.length)
            songid, artistid = results
            
            # insert songplay record
            songplay_data = (pd.Timestamp(row.ts).time(), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
            cur.execute(songplay_table_insert, songplay_data)
        else:
            songid, artistid = None, None
        
        


def process_data(cur, conn, filepath, func):
    """ Find all paths from dataset and call functions to process the data.
    
    Argument1: Connect cursor to connect to database
    Argument2: Connection to data
    Argument3: Path for dataset
    Argument4: Fucntion to process data

    
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
#         if(i==22):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))
        


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()