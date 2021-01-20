import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

# The ETL script reads song_data and load_data from S3,
# transforms them to create five different tables,
# and writes them to partitioned parquet files in table directories on S3.

# Data subset
# You can work on your project with a smaller dataset found in workspace, then move on to bigger dataset on AWS.

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create SparkSession to process extracted S3 data.
    online docs: https://spark.apache.org/docs/latest/sql-getting-started.html

    :return: an existing SparkSession
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    reads JSON file from S3, transforms it,
    and outputs them back to S3 as partitioned parquet files (as fact and dimensional tables)

    :param spark: an existing SparkSession
    :param input_data: S3 bucket directory (e.g., "s3a://udacity-dend/")
    :param output_data: output directory (e.g., 'output/')
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/A/A/B/*.json'                   # TEST
    # song_data = os.path.join(input_data, 'song-data/*/*/*/*.json')    # FINAL

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    # dim table: songs
    songs_table = df['song_id', 'title', 'artist_id', 'year', 'duration']
# or, shall I do style from here -- https://spark.apache.org/docs/latest/sql-getting-started.html
# songs_table = df.select(df['song_id'], df['title'], df['artist_id'], df['year'], df['duration'])

    # write songs table to parquet files partitioned by year and artist
    # songs_table.write.partitionBy('year', 'artist_id').mode('overwrite').parquet(os.path.join(output_data, 'songs'))
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs'))

    # extract columns to create artists table
    # dim table: artists
    artists_table = df['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
# or, shall I do style from here -- https://spark.apache.org/docs/latest/sql-getting-started.html

    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists'))


def process_log_data(spark, input_data, output_data):
    """
    reads JSON file from S3, transforms it,
    and outputs them back to S3 as partitioned parquet files (as fact and dimensional tables)

    :param spark: an existing SparkSession
    :param input_data: S3 bucket directory (e.g., "s3a://udacity-dend/")
    :param output_data: output directory (e.g., 'output/')
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/2018/11/*.json'               # TEST
    # log_data = os.path.join(input_data, 'log_data/*/*/*.json')    # FINAL

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df['page'] == 'NextSong')

    # extract columns for users table
    # dim table: users
    users_table = df['user_id', 'first_name', 'last_name', 'gender', 'level']
# or, shall I do style from here -- https://spark.apache.org/docs/latest/sql-getting-started.html
# if a fail point occurs: friend's code has an extra line here: dropDuplicates(['user_id'])  or dropDuplicates()

    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users'))

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = None # TODO: add in timestamp column

    # create datetime column from original timestamp column
    get_datetime = udf()
    df = None # TODO: add in datetime column

    # extract columns to create time table
    # dim table: time
    time_table = df['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
# or, shall I do style from here -- https://spark.apache.org/docs/latest/sql-getting-started.html
#     TODO -- note: this may be more involved, due to datetime complexities!

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'time'))

    # read in song data to use for songplays table
    # https://knowledge.udacity.com/questions/439032
    song_df = None # TODO

    # extract columns from joined song and log datasets to create songplays table
    # fact table: songplays
    # TODO: incorporate JOIN, of song and log datasets
    songplays_table = df['songplay_id', 'start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent']

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'songplays'))
#    TODO -- see rubric / instructions -- this may be a LOT more involved


def main():
    """main execution function"""
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "output/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
