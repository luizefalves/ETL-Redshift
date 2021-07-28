import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

# etl.py load data from S3 to staging tables on Redshift.
# and load data from staging tables to analytics tables on Redshift.

def load_staging_tables(cur, conn):
    for query in copy_table_queries:  # copy_table_queries = [staging_events_copy, staging_songs_copy]
        cur.execute(query)
        conn.commit()

# insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

def insert_tables(cur, conn):
    for query in insert_table_queries:  
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()