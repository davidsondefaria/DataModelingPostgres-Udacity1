import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)
    
    # insert song record
    song_data = []
    
    song_data.append(df['song_id'].values[0])
    song_data.append(df['title'].values[0])
    song_data.append(df['artist_id'].values[0])
    song_data.append(df['year'].values[0].item())
    song_data.append(df['duration'].values[0].item())
    
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = []
    artist_data.append(df['artist_id'].values[0])
    artist_data.append(df['artist_name'].values[0])
    artist_data.append(df['artist_location'].values[0])
    artist_data.append(df['artist_latitude'].values[0].item())
    artist_data.append(df['artist_longitude'].values[0].item())

    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    dropIndex = []
    for i, row in df.iterrows():
        if(row['page'] != 'NextSong'):
            dropIndex.append(i)
    df.drop(dropIndex, inplace=True)    
    
    # convert timestamp column to datetime
    t = list(map(pd.Timestamp, df['ts']))
    
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
#         print(song_select)
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            print(results, row.song, row.artist, row.length)
            songid, artistid = results
        else:
            songid, artistid = None, None
        
        # insert songplay record
        songplay_data = (pd.Timestamp(row.ts).time(), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
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