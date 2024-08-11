from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
import boto3
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Constants for S3 bucket and file paths
BUCKET_NAME = 'nl-aws-de-labs'
SONGS_FILE_PATH = 'spotify_data/songs.csv'
USERS_FILE_PATH = 'spotify_data/users.csv'
STREAMS_PREFIX = 'spotify_data/streams/'
ARCHIVE_PREFIX = 'spotify_data/streams/archived/'

REQUIRED_COLUMNS = {
    'songs': ['id', 'track_id', 'artists', 'album_name', 'track_name', 'popularity',
              'duration_ms', 'explicit', 'danceability', 'energy', 'key', 'loudness',
              'mode', 'speechiness', 'acousticness', 'instrumentalness', 'liveness',
              'valence', 'tempo', 'time_signature', 'track_genre'],
    'streams': ['user_id', 'track_id', 'listen_time'],
    'users': ['user_id', 'user_name', 'user_age', 'user_country', 'created_at']
}

def list_s3_files(prefix, bucket=BUCKET_NAME):
    """ List all files in S3 bucket that match the prefix """
    s3 = boto3.client('s3')
    try:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        files = [content['Key'] for content in response.get('Contents', []) if content['Key'].endswith('.csv')]
        logging.info(f"Successfully listed files with prefix {prefix} from S3: {files}")
        return files
    except Exception as e:
        logging.error(f"Failed to list files with prefix {prefix} from S3: {str(e)}")
        raise Exception(f"Failed to list files with prefix {prefix} from S3: {str(e)}")

def read_s3_csv(file_name, bucket=BUCKET_NAME):
    """ Helper function to read a CSV file from S3 """
    s3 = boto3.client('s3')
    try:
        obj = s3.get_object(Bucket=bucket, Key=file_name)
        logging.info(f"Successfully read {file_name} from S3")
        return pd.read_csv(obj['Body'])
    except Exception as e:
        logging.error(f"Failed to read {file_name} from S3: {str(e)}")
        raise Exception(f"Failed to read {file_name} from S3: {str(e)}")

def validate_datasets():
    validation_results = {}

    # Validate songs dataset
    try:
        songs_data = read_s3_csv(SONGS_FILE_PATH)
        missing_columns = set(REQUIRED_COLUMNS['songs']) - set(songs_data.columns)
        if not missing_columns:
            validation_results['songs'] = True
            logging.info("All required columns present in songs")
        else:
            validation_results['songs'] = False
            logging.warning(f"Missing columns in songs: {missing_columns}")
    except Exception as e:
        validation_results['songs'] = False
        logging.error(f"Failed to read or validate songs from S3: {e}")
        raise

    # Validate users dataset
    try:
        users_data = read_s3_csv(USERS_FILE_PATH)
        missing_columns = set(REQUIRED_COLUMNS['users']) - set(users_data.columns)
        if not missing_columns:
            validation_results['users'] = True
            logging.info("All required columns present in users")
        else:
            validation_results['users'] = False
            logging.warning(f"Missing columns in users: {missing_columns}")
    except Exception as e:
        validation_results['users'] = False
        logging.error(f"Failed to read or validate users from S3: {e}")
        raise

    # Validate streams datasets
    try:
        stream_files = list_s3_files(STREAMS_PREFIX)
        for stream_file in stream_files:
            streams_data = read_s3_csv(stream_file)
            missing_columns = set(REQUIRED_COLUMNS['streams']) - set(streams_data.columns)
            if not missing_columns:
                validation_results['streams'] = True
                logging.info(f"All required columns present in {stream_file}")
            else:
                validation_results['streams'] = False
                logging.warning(f"Missing columns in {stream_file}: {missing_columns}")
                break
    except Exception as e:
        validation_results['streams'] = False
        logging.error(f"Failed to read or validate streams from S3: {e}")
        raise

    return validation_results

def branch_task(ti):
    validation_results = ti.xcom_pull(task_ids='validate_datasets')
    
    if all(validation_results.values()):
        return 'calculate_genre_level_kpis'
    else:
        return 'end_dag'

def upsert_to_redshift(df, table_name, id_columns):
    redshift_hook = PostgresHook(postgres_conn_id="redshift_default")
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()

    try:
        # Convert DataFrame to list of tuples
        data_tuples = [tuple(x) for x in df.to_numpy()]

        # Create insert query for the temporary table
        cols = ', '.join(list(df.columns))
        vals = ', '.join(['%s'] * len(df.columns))
        tmp_table_query = f"INSERT INTO reporting_schema.tmp_{table_name} ({cols}) VALUES ({vals})"

        cursor.executemany(tmp_table_query, data_tuples)

        # Create the merge (upsert) query
        delete_condition = ' AND '.join([f'tmp_{table_name}.{col} = {table_name}.{col}' for col in id_columns])
        merge_query = f"""
        BEGIN;
        DELETE FROM reporting_schema.{table_name}
        USING reporting_schema.tmp_{table_name}
        WHERE {delete_condition};

        INSERT INTO reporting_schema.{table_name}
        SELECT * FROM reporting_schema.tmp_{table_name};

        TRUNCATE TABLE reporting_schema.tmp_{table_name};
        COMMIT;
        """

        cursor.execute(merge_query)
        conn.commit()
        logging.info(f"Data ingested and merged successfully into {table_name}")
    except Exception as e:
        conn.rollback()
        logging.error(f"Failed to ingest and merge data into {table_name}: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def calculate_genre_level_kpis():
    stream_files = list_s3_files(STREAMS_PREFIX)
    streams_data = pd.concat([read_s3_csv(file) for file in stream_files], ignore_index=True)
    songs_data = read_s3_csv(SONGS_FILE_PATH)

    streams_data['listen_date'] = pd.to_datetime(streams_data['listen_time']).dt.date
    merged_data = streams_data.merge(songs_data, on='track_id', how='left')

    genre_listen_count = merged_data.groupby(['listen_date', 'track_genre']).size().reset_index(name='listen_count')
    merged_data['duration_seconds'] = merged_data['duration_ms'] / 1000
    avg_duration = merged_data.groupby(['listen_date', 'track_genre'])['duration_seconds'].mean().reset_index(name='average_duration')
    total_listens = merged_data.groupby('listen_date').size().reset_index(name='total_listens')
    genre_listen_count = genre_listen_count.merge(total_listens, on='listen_date')
    genre_listen_count['popularity_index'] = genre_listen_count['listen_count'] / genre_listen_count['total_listens']

    most_popular_track = merged_data.groupby(['listen_date', 'track_genre', 'track_id']).size().reset_index(name='track_count')
    most_popular_track = most_popular_track.sort_values(by=['listen_date', 'track_genre', 'track_count'], ascending=[True, True, False])
    most_popular_track = most_popular_track.drop_duplicates(subset=['listen_date', 'track_genre'], keep='first').rename(columns={'track_id': 'most_popular_track_id'})

    final_kpis = genre_listen_count[['listen_date', 'track_genre', 'listen_count', 'popularity_index']]
    final_kpis = final_kpis.merge(avg_duration, on=['listen_date', 'track_genre'])
    final_kpis = final_kpis.merge(most_popular_track[['listen_date', 'track_genre', 'most_popular_track_id']], on=['listen_date', 'track_genre'])

    logging.info("Genre-level KPIs:")
    logging.info(final_kpis.columns)

    upsert_to_redshift(final_kpis, 'genre_level_kpis', ['listen_date', 'track_genre'])

def calculate_hourly_kpis():

    stream_files = list_s3_files(STREAMS_PREFIX)
    streams_data = pd.concat([read_s3_csv(file) for file in stream_files], ignore_index=True)
    songs_data = read_s3_csv(SONGS_FILE_PATH)
    users_data = read_s3_csv(USERS_FILE_PATH)

    streams_data['listen_date'] = pd.to_datetime(streams_data['listen_time']).dt.date
    streams_data['listen_hour'] = pd.to_datetime(streams_data['listen_time']).dt.hour
    full_data = streams_data.merge(songs_data, on='track_id').merge(users_data, on='user_id')

    # KPI 1: Hourly Unique Listeners
    hourly_unique_listeners = full_data.groupby(['listen_date', 'listen_hour'])['user_id'].nunique().reset_index(name='unique_listeners')

    # KPI 2: Top Listened Artist of the Hour
    artist_listen_counts = full_data.groupby(['listen_date', 'listen_hour', 'artists']).size().reset_index(name='listen_counts')
    top_artist = artist_listen_counts.loc[artist_listen_counts.groupby(['listen_date', 'listen_hour'])['listen_counts'].idxmax()]
    top_artist = top_artist.rename(columns={'artists': 'top_artist'})

    # KPI 3: Listening Sessions per User per Hour
    full_data['session_id'] = full_data['user_id'].astype(str) + '-' + full_data['listen_time'].astype(str)
    sessions_per_user = full_data.groupby(['listen_date', 'listen_hour', 'user_id']).nunique('session_id').reset_index()
    avg_sessions_per_user = sessions_per_user.groupby(['listen_date', 'listen_hour'])['session_id'].mean().reset_index(name='avg_sessions_per_user')

    # KPI 4: Hourly Track Diversity Index
    track_diversity = full_data.groupby(['listen_date', 'listen_hour'])['track_id'].agg(['nunique', 'count']).reset_index()
    track_diversity['diversity_index'] = track_diversity['nunique'] / track_diversity['count']

    # KPI 5: Most Engaged User Group by Age per Hour
    users_data['age_group'] = pd.cut(users_data['user_age'], bins=[0, 25, 35, 45, 55, 65, 100], labels=['18-25', '26-35', '36-45', '46-55', '56-65', '66+'])
    user_group_engagement = full_data.merge(users_data, on='user_id').groupby(['listen_date', 'listen_hour', 'age_group']).size().reset_index(name='streams')
    most_engaged_group = user_group_engagement.loc[user_group_engagement.groupby(['listen_date', 'listen_hour'])['streams'].idxmax()].rename(columns={'age_group': 'most_engaged_age_group'})

    # Combine all KPIs into one DataFrame
    final_kpis = hourly_unique_listeners.merge(top_artist, on=['listen_date', 'listen_hour'])
    final_kpis = final_kpis.merge(avg_sessions_per_user, on=['listen_date', 'listen_hour'])
    final_kpis = final_kpis.merge(track_diversity[['listen_date', 'listen_hour', 'diversity_index']], on=['listen_date', 'listen_hour'])
    final_kpis = final_kpis.merge(most_engaged_group[['listen_date', 'listen_hour', 'most_engaged_age_group']], on=['listen_date', 'listen_hour'])

    # Handle potential column conflicts from merges
    final_kpis = final_kpis.rename(columns={'listen_counts_x': 'listen_counts'})

    # Select the final columns explicitly to match the Redshift table schema
    final_kpis = final_kpis[['listen_date', 'listen_hour', 'unique_listeners', 'listen_counts', 'top_artist',
                             'avg_sessions_per_user', 'diversity_index', 'most_engaged_age_group']]

    logging.info("Hourly KPIs:")
    logging.info(final_kpis.columns)

    upsert_to_redshift(final_kpis, 'hourly_kpis', ['listen_date', 'listen_hour'])

def move_processed_files():
    s3 = boto3.client('s3')
    try:
        stream_files = list_s3_files(STREAMS_PREFIX)
        for file in stream_files:
            copy_source = {'Bucket': BUCKET_NAME, 'Key': file}
            destination_key = file.replace('spotify_data/streams/', 'spotify_data/streams/archived/')
            s3.copy_object(CopySource=copy_source, Bucket=BUCKET_NAME, Key=destination_key)
            s3.delete_object(Bucket=BUCKET_NAME, Key=file)
            logging.info(f"Moved {file} to {destination_key}")
    except Exception as e:
        logging.error(f"Failed to move files from {STREAMS_PREFIX} to {ARCHIVE_PREFIX}: {str(e)}")
        raise

with DAG('data_validation_and_kpi_computation', default_args=default_args, schedule_interval='@daily') as dag:
    validate_datasets = PythonOperator(
        task_id='validate_datasets',
        python_callable=validate_datasets
    )

    check_validation = BranchPythonOperator(
        task_id='check_validation',
        python_callable=branch_task,
        provide_context=True
    )

    calculate_genre_level_kpis = PythonOperator(
        task_id='calculate_genre_level_kpis',
        python_callable=calculate_genre_level_kpis
    )

    calculate_hourly_kpis = PythonOperator(
        task_id='calculate_hourly_kpis',
        python_callable=calculate_hourly_kpis
    )

    move_files = PythonOperator(
        task_id='move_processed_files',
        python_callable=move_processed_files
    )

    end_dag = DummyOperator(
        task_id='end_dag'
    )

    validate_datasets >> check_validation >> [calculate_genre_level_kpis,end_dag]
    calculate_genre_level_kpis >> calculate_hourly_kpis>>move_files