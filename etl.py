import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format, from_unixtime
from pyspark.sql.types import LongType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    # assumes songs are stored in a tree hierarchy input_data/<X>/<Y>/<Z>
    # with <XYZ> being the 1st 3 letters of the song track ID
    # X,Y,Z can be A,B or C
    song_data = os.path.join(input_data, "{A,B,C}","{A,B,C}","{A,B,C}")
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", 
                      "title", 
                      "artist_id", 
                      "year", 
                      "duration")
    
    # write songs table to parquet files partitioned by year and artist
    out_song = os.path.join(output_data, "ARTIST")
    songs_table.write.partitionBy("year", "artist_id").mode("overwrite").parquet(out_song)

    # extract columns to create artists table
    artists_table = df.select("artist_id", 
                        col("artist_name").alias("name"),
                        col("artist_location").alias("location"),
                        col("artist_latitude").alias("latitude"),
                        col("artist_longitude").alias("longitude") 
                    ).distinct()
    
    # write artists table to parquet files
    out_artist = os.path.join(output_data, "SONG")
    artists_table.write.mode("overwrite").parquet(out_artist)


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data")

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter("page != 'NextSong' ")

    # extract columns for users table    
    users_table = df.select("userId", 
                         "firstName", 
                         "lastName", 
                         "gender", 
                         "level")\
                    .distinct()
    # rename columns and cast user id to int
    users_table = users_table.select( col("userId").cast("long").alias("user_id"),
                          col("firstName").alias("first_name"),
                          col("lastName").alias("last_name"),
                          "gender",
                          "level"
                          )\
                .orderBy("user_id")
    
    # write users table to parquet files
    out_users = os.path.join(output_data, "USERS")
    users_table.write.mode("overwrite").parquet(out_users)

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : int(x / 1000.), LongType() )
    df = df.withColumn("timestamp", get_timestamp("ts"))
    
    # create datetime column from original timestamp column
    #get_datetime = udf()
    df = df.withColumn("datetime", from_unixtime("timestamp"))
    
    # extract columns to create time table
    time_table = df.select("ts", "datetime")\
            .withColumn("hour", hour("datetime"))\
            .withColumn("day", dayofmonth("datetime"))\
            .withColumn("week", weekofyear("datetime"))\
            .withColumn("month", month("datetime"))\
            .withColumn("year", year("datetime"))\
            .withColumn("weekday", dayofweek("datetime"))\
            .distinct()
    time_table = time_table.drop("datetime")

    # write time table to parquet files partitioned by year and month
    out_time = os.path.join(output_data, "TIMESTAMP")
    time_table.write.partitionBy("year", "month").mode("overwrite").parquet(out_time)

    # read in song data to use for songplays table
    song_df = ""

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = ""

    # write songplays table to parquet files partitioned by year and month
    #songplays_table


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

def main_1():
    spark = create_spark_session()
    input_data = "song_data"
    output_data="OUT"
    #process_song_data(spark, input_data, output_data) 
    input_data = "./"
    process_log_data(spark, input_data, output_data)
    
if __name__ == "__main__":
    main_1()
