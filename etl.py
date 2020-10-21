import warnings
import configparser
import psycopg2
import pandas as pd
from ast import literal_eval
from dateutil.parser import parse
import boto3
import io
import time
from sys import exit
from sql_queries import (insert_table_queries, create_table_queries,
                         drop_table_queries)

warnings.filterwarnings("ignore")


def get_matching_s3_keys(bucket, s3c, prefix='', suffix=''):
    """
    Generate the keys in an S3 bucket.

    Input:
        bucket - Name of the S3 bucket.
        s3c - S3 Cliente connection.
        prefix - Only fetch keys that start with this prefix (optional).
        suffix - Only fetch keys that end with this suffix (optional).
    Return:
        key - list of objects on S3 bucket
    """

    kwargs = {'Bucket': bucket}

    # If the prefix is a single string (not a tuple of strings), we can
    # do the filtering directly in the S3 API.
    if isinstance(prefix, str):
        kwargs['Prefix'] = prefix

    while True:

        # The S3 API response is a large blob of metadata.
        # 'Contents' contains information about the listed objects.
        resp = s3c.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            key = obj['Key']
            if key.startswith(prefix) and key.endswith(suffix):
                yield key

        # The S3 API is paginated, returning up to 1000 keys at a time.
        # Pass the continuation token into the next response, until we
        # reach the final page (when this field is missing).
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break


def copy_csv_to_s3(client, df, bucket, filepath):
    """
    Copy pandas df to S3 as csv file.

    Input:
        client - S3 client conection.
        df - Pandas df to be copied into S3 as csv file.
        bucket - S3 bucket where data will be stored.
        filepath - path where data will be stored
    Return:
        N/A
    """

    csv_buf = io.StringIO()
    df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    client.put_object(Bucket=bucket, Body=csv_buf.getvalue(), Key=filepath)


def read_anime_df(df, suffix, BUCKET_NAME, path, s3c):
    """
    Transform animes raw data and copy it to S3 as csv file.

    Input:
        df - Pandas df with anime raw data.
        suffix - csv file sufiix.
        BUCKET_NAME - S3 bucket where data will be stored.
        path - path where data will be stored
        s3c - S3 Cliente connection.
        count_s3[] - Quantity of lines on pandas df (Global)
    Return:
        N/A
    """

    # Create data frame structure of dimensions tables
    df_anime = pd.DataFrame(columns=['animeID', 'title', 'title_english',
                                     'type', 'source', 'episodes', 'status',
                                     'duration', 'rating', 'score',
                                     'scored_by', 'members', 'favorites',
                                     'aired_from', 'aired_to', 'premiered',
                                     'broadcast'])
    df_relations = pd.DataFrame(columns=['animeID', 'relation', 'id_relation',
                                'relation_type'])
    df_producers = pd.DataFrame(columns=['animeID', 'producer_id',
                                'producer_name'])
    df_licensors = pd.DataFrame(columns=['animeID', 'licensor_id',
                                'licensor_name'])
    df_studios = pd.DataFrame(columns=['animeID', 'studio_id', 'studio_name'])
    df_genres = pd.DataFrame(columns=['animeID', 'genre_id', 'genre_name'])

    # Iterate through df transforming the data
    for i, row in df.iterrows():

        # Transform aired dictionary into aired_from data
        aux_aired_from = literal_eval(row. aired)['from']

        if aux_aired_from is not None:
            aired_from = parse(aux_aired_from).date()
        else:
            aired_from = aux_aired_from

        # Transform aired string into dicitionary and then into aired_from data
        aux_aired_to = literal_eval(row.aired)['to']

        if aux_aired_to is not None:
            aired_to = parse(aux_aired_to).date()
        else:
            aired_to = aux_aired_to

        # Append trasformed data into df_anime
        df_anime = df_anime.append(pd.DataFrame([[row.animeID, row.title,
                                                row.title_english, row.type,
                                                row.source, row.episodes,
                                                row.status, row.duration,
                                                row.rating, row.score,
                                                row.scored_by, row.members,
                                                row.favorites, aired_from,
                                                aired_to, row.premiered,
                                                row.broadcast]],
                                                columns=['animeID', 'title',
                                                         'title_english',
                                                         'type', 'source',
                                                         'episodes', 'status',
                                                         'duration', 'rating',
                                                         'score', 'scored_by',
                                                         'members',
                                                         'favorites',
                                                         'aired_from',
                                                         'aired_to',
                                                         'premiered',
                                                         'broadcast']),
                                   ignore_index=True)

        # Transform related string into dictionary
        aux_relations = literal_eval(row.related)
        # Iterate through dictionary elements
        for relation in aux_relations:
            # Iterate through list elements inside dictionary
            for j in range(0, len(aux_relations[relation])):
                # Append trasformed data into df_relations
                df_relations = df_relations.append(pd.DataFrame([[row.animeID,
                                                                  relation,
                                                                  aux_relations[relation][j]['mal_id'],
                                                                  aux_relations[relation][j]['type']]],
                                                                columns=['animeID',
                                                                         'relation',
                                                                         'id_relation',
                                                                         'relation_type']),
                                                   ignore_index=True)

        # Transform producer string into list
        aux_producers = literal_eval(row.producer)
        for j in range(0, len(aux_producers)):
            # Append trasformed data into df_producers
            df_producers = df_producers.append(pd.DataFrame([[row.animeID,
                                                              aux_producers[j]['mal_id'],
                                                              aux_producers[j]['name']]],
                                                            columns=['animeID',
                                                                     'producer_id',
                                                                     'producer_name']),
                                               ignore_index=True)

        # Transform licensor string into list
        aux_licensors = literal_eval(row.licensor)
        for j in range(0, len(aux_licensors)):
            # Append trasformed data into df_licensors
            df_licensors = df_licensors.append(pd.DataFrame([[row.animeID,
                                                              aux_licensors[j]['mal_id'],
                                                              aux_licensors[j]['name']]],
                                                            columns=['animeID',
                                                                     'licensor_id',
                                                                     'licensor_name']),
                                               ignore_index=True)

        # Transform studio string into list
        aux_studios = literal_eval(row.studio)
        for j in range(0, len(aux_studios)):
            # Append trasformed data into df_studios
            df_studios = df_studios.append(pd.DataFrame([[row.animeID,
                                                          aux_studios[j]['mal_id'],
                                                          aux_studios[j]['name']]],
                                                        columns=['animeID',
                                                                 'studio_id',
                                                                 'studio_name']),
                                           ignore_index=True)

        # Transform genre string into list
        aux_genres = literal_eval(row.genre)
        for j in range(0, len(aux_genres)):
            # Append trasformed data into df_studios
            df_genres = df_genres.append(pd.DataFrame([[row.animeID,
                                                        aux_genres[j]['mal_id'],
                                                        aux_genres[j]['name']]],
                                                      columns=['animeID',
                                                               'genre_id',
                                                               'genre_name']),
                                         ignore_index=True)

    # Transform df to csv and stores it into S3
    copy_csv_to_s3(client=s3c, df=df_anime, bucket=BUCKET_NAME,
                   filepath=path + 'animes_' + str(suffix) + '.csv')
    copy_csv_to_s3(client=s3c, df=df_relations, bucket=BUCKET_NAME,
                   filepath=path + 'anime_relations_' + str(suffix) + '.csv')
    copy_csv_to_s3(client=s3c, df=df_producers, bucket=BUCKET_NAME,
                   filepath=path + 'producers_' + str(suffix) + '.csv')
    copy_csv_to_s3(client=s3c, df=df_licensors, bucket=BUCKET_NAME,
                   filepath=path + 'licensors_' + str(suffix) + '.csv')
    copy_csv_to_s3(client=s3c, df=df_studios, bucket=BUCKET_NAME,
                   filepath=path + 'studios_' + str(suffix) + '.csv')
    copy_csv_to_s3(client=s3c, df=df_genres, bucket=BUCKET_NAME,
                   filepath=path + 'genres_' + str(suffix) + '.csv')

    # Accumulate count s3 to each table
    count_s3[0] += df_anime.shape[0]
    count_s3[1] += df_relations.shape[0]
    count_s3[2] += df_producers.shape[0]
    count_s3[3] += df_licensors.shape[0]
    count_s3[4] += df_studios.shape[0]
    count_s3[5] += df_genres.shape[0]


def read_AnimeList_df(df, KEY, BUCKET_NAME, path, s3c):
    """
    Transform anime list raw data and copy it to S3 as csv file.

    Input:
        df - Pandas df with anime raw data.
        KEY - path that raw data are stored.
        BUCKET_NAME - S3 bucket where data will be stored.
        path - path where data will be stored
        s3c - S3 Cliente connection.
    Return:
        N/A
    """
    # Using KEY input, finds file name
    file = KEY.split("/")[-1].replace('.json', '.csv')

    # Downloaded anime list has a pattern username_animelistpage
    # Since there is not an column with user name on json file, it's used part
    # of the file name to identify it.
    username = file[0:file.rfind('_')]

    # Create data frame structure of staging table
    df_AnimeList = pd.DataFrame(columns=['username', 'animeID',
                                         'watching_status', 'score',
                                         'watched_episodes', 'is_rewatching',
                                         'watch_start_date', 'watch_end_date',
                                         'days', 'priority'])

    # Iterate through df transforming the data
    for i, row in df.iterrows():

        # Iterate trought anime list of dictionaries and transform data
        for j in range(0, len(row.anime)):
            if row.anime[j]['watch_start_date'] is not None:
                watch_start_date = parse(row.anime[j]['watch_start_date']).date()
            else:
                watch_start_date = row.anime[j]['watch_start_date']

            if row.anime[j]['watch_end_date'] is not None:
                watch_end_date = parse(row.anime[j]['watch_end_date']).date()
            else:
                watch_end_date = row.anime[j]['watch_end_date']

            # Append trasformed data into df_AnimeList
            df_AnimeList = df_AnimeList.append(pd.DataFrame([[username,
                                                              row.anime[j]['mal_id'],
                                                              row.anime[j]['watching_status'],
                                                              row.anime[j]['score'],
                                                              row.anime[j]['watched_episodes'],
                                                              row.anime[j]['is_rewatching'],
                                                              watch_start_date,
                                                              watch_end_date,
                                                              row.anime[j]['days'],
                                                              row.anime[j]['priority']]],
                                                            columns=['username',
                                                                     'animeID',
                                                                     'watching_status',
                                                                     'score',
                                                                     'watched_episodes',
                                                                     'is_rewatching',
                                                                     'watch_start_date',
                                                                     'watch_end_date',
                                                                     'days',
                                                                     'priority']),
                                               ignore_index=True)

    # Transform df to csv and stores it into S3
    copy_csv_to_s3(client=s3c, df=df_AnimeList, bucket=BUCKET_NAME,
                   filepath=path + 'staging_' + file)

    # Accumulate count s3 to Animelist table
    count_s3[6] += df_AnimeList.shape[0]


def read_user_df(df, KEY, BUCKET_NAME, path, s3c):
    """
    Transform users raw data and copy it to S3 as csv file.

    Input:
        df - Pandas df with anime raw data.
        KEY - path that raw data are stored.
        BUCKET_NAME - S3 bucket where data will be stored.
        path - path where data will be stored
        s3c - S3 Cliente connection.
    Return:
        N/A
    """
    # Using KEY input, finds file name
    file = KEY.split("/")[-1].replace('.json', '.csv')

    # Transform date columns on users df
    df_user = df[['user_id', 'username', 'last_online', 'gender', 'birthday',
                  'location', 'joined']]
    df_user['birthday'] = pd.to_datetime(df_user['birthday'], format='%Y-%m-%d')
    df_user['joined'] = pd.to_datetime(df_user['joined'], format='%Y-%m-%d')
    df_user['last_online'] = pd.to_datetime(df_user['last_online'],
                                            format='%Y-%m-%d %H:%M:%S')

    # create favorite df structure
    df_favorite_anime = pd.DataFrame(columns=['username', 'animeID'])

    # iterate trought df and transform data
    for i, row in df.iterrows():
        aux_favorite = row.favorites['anime']
        for j in range(0, len(aux_favorite)):
            # append data into df_favorite_anime
            df_favorite_anime = df_favorite_anime.append(pd.DataFrame([[row.username,
                                                                        aux_favorite[j]['mal_id']]],
                                                                      columns=['username',
                                                                               'animeID']),
                                                         ignore_index=True)

    # Transform df to csv and stores it into S3
    copy_csv_to_s3(client=s3c, df=df_favorite_anime, bucket=BUCKET_NAME,
                   filepath=path + 'staging_favorite_' + file)
    copy_csv_to_s3(client=s3c, df=df_user, bucket=BUCKET_NAME,
                   filepath=path + 'users_' + file)

    # Accumulate count s3 to each table
    count_s3[7] += df_favorite_anime.shape[0]
    count_s3[8] += df_user.shape[0]


def drop_tables(cur, conn):
    """
    Drop existing tables from AWS redshift

    Input:
        cur - cursory to connected DB.
        conn - psycopg2 connection to Postgres database.
    Return:
        N/A
    """
    for query in drop_table_queries:
        print(F"Executing query '{query}'")
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create tables into AWS redshift

    Input:
        cur - cursory to connected DB.
        conn - psycopg2 connection to Postgres database.
    Return:
        N/A
    """
    for query in create_table_queries:
        print(F"Executing query '{query}'")
        cur.execute(query)
        conn.commit()
        print('------------------')


def copy_tables_redshift(cur, conn, ARN, path, file_format, copy_to,
                         copy_from='', IGNOREHEADER=True):
    """
    Load S3 data into AWS Redshift

    Input:
        cur - cursory to connected DB.
        conn - psycopg2 connection to Postgres database.
        ARN - ARN allowed in Redshift cluster.
        path - S3 input path.
        file_format - ouput file format.
        copy_to - redshift table who data will be copied.
        copy_from - table's prefix name who will be copied (optional).
        IGNOREHEADER - option of ignore header when copy to Redshift (optional).
    Return:
        N/A
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    BUCKET_NAME = config['S3']['BUCKET']
    REGION = config['S3']['REGION']

    staging_copy = (F"""
        copy {copy_to} from 's3://{BUCKET_NAME}/{path}{copy_from}'
        credentials 'aws_iam_role={ARN}'
        format as {file_format}
        ACCEPTINVCHARS AS '^'
        STATUPDATE ON
        region '{REGION}'
    """)
    if IGNOREHEADER:
        staging_copy += ' IGNOREHEADER 1;'
    else:
        staging_copy += ';'

    print(F"Copying '{copy_to}' table")
    cur.execute(staging_copy)
    conn.commit()


def insert_tables(cur, conn):
    """
    Create fact table using staging tables

    Input:
        cur - cursory to connected DB.
        conn - psycopg2 connection to Postgres database.
    Return:
        N/A
    """
    for query in insert_table_queries:
        print('Query: {}'.format(query))
        cur.execute(query)
        conn.commit()
        print('------------------')


def count_query(cur, conn, table, table_key):
    """
    Count quantity of lines on table

    Input:
        cur - cursory to connected DB.
        conn - psycopg2 connection to Postgres database.
        table - table name
    Return:
        count - quantity of lines on table
    """
    query = F"SELECT COUNT(*) FROM {table}"
    query += F" WHERE {table_key[0]} IS NOT NULL"

    i = 1
    while i < len(table_key):
        query += F" AND {table_key[i]} IS NOT NULL"
        i += 1
    query += ";"
    print('Query: {}'.format(query))
    row = cur.execute(query)
    count = cur.fetchone()[0]
    return count


def main():

    start_time = time.time()
    print('------------------')

    # Read config file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    ACCESS_KEY_ID = config['AWS']['KEY']
    SECRET_ACCESS_KEY = config['AWS']['SECRET']
    BUCKET_NAME = config['S3']['BUCKET']
    REGION = config['S3']['REGION']
    ANIME_INPUT = config['S3']['ANIME_INPUT']
    ANIME_OUTPUT = config['S3']['ANIME_OUTPUT']
    ANIME_LIST_INPUT = config['S3']['ANIME_LIST_INPUT']
    ANIME_LIST_OUTPUT = config['S3']['ANIME_LIST_OUTPUT']
    USER_INPUT = config['S3']['USER_INPUT']
    USER_OUTPUT = config['S3']['USER_OUTPUT']

    global count_s3
    count_s3 = [0, 0, 0, 0, 0, 0, 0, 0, 0]  # count quantity lines on s3 tables

    """
    Position 0 - animes table
    Position 1 - anime_relations table
    Position 2 - producers table
    Position 3 - licensors table
    Position 4 - studios table
    Position 5 - genres table
    Position 6 - staging_anime_list table
    Position 7 - staging_favorite table
    Position 8 - users table
    """
    all_tables = ['animes', 'anime_relations', 'producers', 'licensors',
                  'studios', 'genres', 'staging_anime_list',
                  'staging_favorite', 'users']
    tables_keys = {
                   "animes": ['animeID'],
                   "anime_relations": ['animeID'],
                   "producers": ['animeID'],
                   "licensors": ['animeID'],
                   "studios": ['animeID'],
                   "genres": ['animeID'],
                   "staging_anime_list": ['animeID', 'username'],
                   "staging_favorite": ['animeID', 'username'],
                   "users": ['username']
                  }

    # -------------------------------------------------------------------------
    # Iniciate process of transform raw data and store it into S3
    print("Trying connection to S3")
    s3c = boto3.client(
        's3',
        region_name=REGION,
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=SECRET_ACCESS_KEY
    )

    print("Connected to S3")
    print('------------------')

    transform_raw_data_time = time.time()

    print("Getting animes objects on S3")
    anime_keys = []
    for key in get_matching_s3_keys(bucket=BUCKET_NAME, prefix=ANIME_INPUT,
                                    s3c=s3c):
        anime_keys.append(key)

    print("Transforming anime raw data")
    for count in range(len(anime_keys)):
        obj = s3c.get_object(Bucket=BUCKET_NAME, Key=anime_keys[count])
        df = pd.read_csv(io.BytesIO(obj['Body'].read()), sep=',',
                         delimiter=",", engine='python', encoding='utf-8')
        df.columns = df.columns.str.replace(' ', '')
        read_anime_df(df, count, BUCKET_NAME, ANIME_OUTPUT, s3c)

    print("Anime raw data transformed and copied to S3")
    print('------------------')

    print("Getting anime list objects on S3")
    animelist_keys = []
    for key in get_matching_s3_keys(bucket=BUCKET_NAME,
                                    prefix=ANIME_LIST_INPUT, suffix='.json',
                                    s3c=s3c):
        animelist_keys.append(key)

    print("Transforming anime list raw data")
    for KEY in animelist_keys:
        obj = s3c.get_object(Bucket=BUCKET_NAME, Key=KEY)
        df = pd.read_json(io.BytesIO(obj['Body'].read()), lines=True)
        read_AnimeList_df(df, KEY, BUCKET_NAME, ANIME_LIST_OUTPUT, s3c)

    print("Anime List raw data transformed and copied to S3")
    print('------------------')

    print("Getting users objects on S3")
    users_keys = []
    for key in get_matching_s3_keys(bucket=BUCKET_NAME, prefix=USER_INPUT,
                                    suffix='.json', s3c=s3c):
        users_keys.append(key)

    print("Transforming users raw data")
    for KEY in users_keys:
        obj = s3c.get_object(Bucket=BUCKET_NAME, Key=KEY)
        df = pd.read_json(io.BytesIO(obj['Body'].read()), lines=True)
        read_user_df(df, KEY, BUCKET_NAME, USER_OUTPUT, s3c)
    print("User raw data transformed and copied to S3")
    print('------------------')

    quality_data_time = time.time()
    message_print = "\nTransform RAW data process took "
    message_print += str(quality_data_time - transform_raw_data_time)
    message_print += " seconds\n"
    print(message_print)

    # -------------------------------------------------------------------------
    # Initiate S3 data quality check
    print('------------------')
    print("Connecting to Redshift Cluster")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    print(F"Connect to {config.get('CLUSTER','HOST')}")
    ARN = config.get('IAM_ROLE', 'ARN')

    # First create table if then do not exists
    print('------------------')
    print("Creating staging and final tables (if they do not exists)")
    create_tables(cur, conn)

    # Run's data quality check
    data_quality_pass = True
    i = 0
    for table in all_tables:
        print(F"Running data quality of table {table}")
        if count_s3[i] == 0:
            print(F"Data quality fail because S3 table '{table}' is empty.")
            data_quality_pass = False
        else:
            # count quantity of register into redshift
            table_key = tables_keys[table]
            count_redshift = count_query(cur, conn, table, table_key)
            print(F"Query result: {count_redshift} lines")
            print(F"S3 result: {count_s3[i]} lines")
            if count_redshift != 0:
                # It will be accepted an reduction of 1% of data
                delta_data = (count_s3[i]/count_redshift)*100
                if delta_data < 99:
                    message_print = "Data quality fail because S3 table "
                    message_print += F"{str(delta_data)}% data "
                    message_print += F"compared to Redshift table "
                    message_print += "(Acceptable loss of 1%)."
                    print(message_print)
                    print('------------------')
                    data_quality_pass = False
                else:
                    print(F"S3 table '{table}' has passed data quality check.")
                    print('------------------')
            else:
                message_print = F"Redshif table '{table}' is empty. S3 "
                message_print += F"'{table}' has passed data quality check."
                print(message_print)
                print('------------------')
        i += 1

    redshift_data_time = time.time()
    message_print = "Data quality process took"
    message_print += F"{str(redshift_data_time - quality_data_time)} seconds"
    print(message_print)

    if not data_quality_pass:
        conn.close()
        print ("Not all tables passed data quality check.")
        print ("Terminating ETL process.")
        return None
    else:
        print("All tables passed data quality check.\nContinuing ETL process")
        print('------------------')

    # -------------------------------------------------------------------------
    # Initiate process of copy data from S3 to Redshift

    print("Dropping existing tables")
    drop_tables(cur, conn)

    print('------------------')
    print("Creating new staging and final tables")
    create_tables(cur, conn)

    print("Loading stagings tables into S3")
    copy_table = ['animes', 'anime_relations', 'producers', 'licensors',
                  'studios', 'genres']
    for table in copy_table:
        copy_tables_redshift(cur=cur, conn=conn, ARN=ARN, path=ANIME_OUTPUT,
                             file_format='csv', copy_to=table, copy_from=table)

    copy_tables_redshift(cur=cur, conn=conn, ARN=ARN, path=ANIME_LIST_OUTPUT,
                         file_format="csv", copy_to='staging_anime_list')

    copy_table = ['staging_favorite', 'users']
    for table in copy_table:
        copy_tables_redshift(cur=cur, conn=conn, ARN=ARN, path=USER_OUTPUT,
                             file_format="csv", copy_to=table, copy_from=table)

    print("All tables copied\n")
    print('------------------')

    print("Insert data query")
    insert_tables(cur, conn)

    data_quality_redshift = time.time()
    message_print = "Redshift process took "
    message_print += str(data_quality_redshift - redshift_data_time)
    message_print += " seconds\n"

    # -------------------------------------------------------------------------
    # Initiate Redshift data quality check
    data_quality_pass = True
    i = 0
    for table in all_tables:
        print(F"Running Redshift data quality of table {table}")
        # count quantity of register into redshift
        table_key = tables_keys[table]
        count_redshift = count_query(cur, conn, table, table_key)
        print(F"Query result: {count_redshift} lines")
        print(F"S3 result: {count_s3[i]} lines")
        if count_redshift != 0:
            # It will be accepted an reduction of 1% of data
            delta_data_redshift = abs(count_s3[i] - count_redshift)
            if delta_data_redshift != 0:
                message_print = "Data quality fail because "
                message_print += F"Redhsift table '{table}' has "
                message_print += F"{str(delta_data_redshift)} lines different"
                print(message_print)
                print('------------------')
                data_quality_pass = False
            else:
                print(F"Redshifgt table '{table}' has passed data quality.")
                print('------------------')
        i += 1

    if not data_quality_pass:
        conn.close()
        print ("Not all tables passed data quality check.")
        print ("Terminating ETL process.")
        return None

    print(F"ETL process took {str(time.time() - start_time)} seconds\n")
    print("ETL process concluded with sucess")
    conn.close()

if __name__ == "__main__":
    main()
