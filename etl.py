#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format, from_unixtime
from pyspark.sql.types import LongType


# In[ ]:


config = configparser.ConfigParser()
config.read('dl.cfg')


# In[ ]:


os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['SECRET_ACCESS_KEY']


# In[ ]:


def create_spark_session():
    spark = SparkSession         .builder         .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")         .getOrCreate()
    return spark


# In[ ]:


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    # assumes songs are stored in a tree hierarchy input_data/<X>/<Y>/<Z>
    # with <XYZ> being the 1st 3 letters of the song track ID
    # X,Y,Z in {A,...,Z}
    song_data = os.path.join(input_data,"song_data", "*","*","*")
    
    # read song data file
    df = spark.read.json(song_data)

    print("EXTRACT SONGS")
    # extract columns to create songs table
    songs_table = df.select("song_id", 
                      "title", 
                      "artist_id", 
                      "year", 
                      "duration")
    
    # write songs table to parquet files partitioned by year and artist
    out_song = os.path.join(output_data, "SONGS")
    songs_table.write.partitionBy("year", "artist_id").mode("overwrite").parquet(out_song)

    # extract columns to create artists table
    artists_table = df.select("artist_id", 
                        col("artist_name").alias("name"),
                        col("artist_location").alias("location"),
                        col("artist_latitude").alias("latitude"),
                        col("artist_longitude").alias("longitude") 
                    ).distinct()
    
    # write artists table to parquet files
    out_artist = os.path.join(output_data, "ARTISTS")
    artists_table.write.mode("overwrite").parquet(out_artist)


# In[ ]:


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data", "*", "*")

    # read log data file
    df = spark.read.json(log_data)
    print("EXTRACT USERS")
    # filter by actions for song plays
    df = df.filter("page == 'NextSong' ")
    # extract columns for users table    
    users_table = df.select(col("userId").cast("long").alias("user_id"),
                          col("firstName").alias("first_name"),
                          col("lastName").alias("last_name"),
                          "gender",
                          "level"
                          )\
                .distinct()\
                .orderBy("user_id")
     # write users table to parquet files
    out_users = os.path.join(output_data, "USERS")
    users_table.write.mode("overwrite").parquet(out_users)

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : int(x / 1000.), LongType() )
    df = df.withColumn("timestamp", get_timestamp("ts"))
    spark.udf.register("get_timestamp",get_timestamp)
    
    # create datetime column from original timestamp column
    #get_datetime = udf()
    df = df.withColumn("datetime", from_unixtime("timestamp"))            .withColumn("hour", hour("datetime"))            .withColumn("day", dayofmonth("datetime"))            .withColumn("week", weekofyear("datetime"))            .withColumn("month", month("datetime"))            .withColumn("year", year("datetime"))            .withColumn("weekday", dayofweek("datetime"))    
    # extract columns to create time table
    time_table = df.select("ts", "hour", "day", "week", "month", "year", "weekday")           .distinct()

    # write time table to parquet files partitioned by year and month
    out_time = os.path.join(output_data, "TIMESTAMPS")
    time_table.write.partitionBy("year", "month").mode("overwrite").parquet(out_time)

    # read in song data to use for songplays table
    song_db = os.path.join(output_data, "SONGS")
    song_df = spark.read.parquet(song_db)
    
    df.createOrReplaceTempView("lg")
    song_df.createOrReplaceTempView("sg")
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
    SELECT lg.ts AS start_time,
        lg.year AS year,
        lg.month AS month,
        lg.userId AS user_id,
        lg.level,
        sg.song_id,
        sg.artist_id,
        lg.sessionId AS session_id,
        lg.location,
        lg.userAgent AS user_agent    
    FROM lg
    JOIN sg ON sg.title = lg.song
    """)

    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id())
    rearrange_col = songplays_table.schema.names[:]
    rearrange_col.insert( 0, "songplay_id")
    rearrange_col.pop()
    songplays_table = songplays_table.select(*rearrange_col)
    
    # write songplays table to parquet files partitioned by year and month
    out_songplay = os.path.join(output_data, "SONGPLAYS")
    songplays_table.write.partitionBy("year", "month").mode("overwrite").parquet(out_songplay)
    


# In[ ]:


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "./OUT/"
    
    #process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


# In[ ]:


def main_local():
    spark = create_spark_session()
    input_data = "./"
    output_data="OUT"
    print("PROCESS SONGS")
    process_song_data(spark, input_data, output_data) 
    print("PROCESS LOGS")
    process_log_data(spark, input_data, output_data)

def main_test():
    spark = create_spark_session()
    df_users = spark.read.parquet("OUT/USERS")
    df_songs = spark.read.parquet("OUT/SONGS")
    df_artists = spark.read.parquet("OUT/ARTISTS")
    df_songplays = spark.read.parquet("OUT/SONGPLAYS")
    df_timestamps = spark.read.parquet("OUT/TIMESTAMPS")
    
    print("users : ", df_users.count())
    print("songs : ", df_songs.count())
    print("artists : ", df_artists.count())
    print("timestamps : ", df_timestamps.count())
    print("songplays : ", df_songplays.count())
# In[ ]:


if __name__ == "__main__":
    #main_local()
    main()
    main_test()



