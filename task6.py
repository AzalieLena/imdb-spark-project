"""
   Get information about how many episodes in each TV Series.
   Get the top 50 of them starting from the TV Series with the biggest quantity of episodes.
"""

import pyspark.sql.types as t
from pyspark.sql.functions import col, row_number, max
from read_write import read, write
from pyspark.sql.window import Window


def read_episodes(spark_session):
    df_path = './imdb-data/title.episode.tsv.gz'
    df_schema = t.StructType([t.StructField('tconst', t.StringType(), False),
                              t.StructField('parentTconst', t.StringType(), False),
                              t.StructField('seasonNumber', t.IntegerType(), True),
                              t.StructField('episodeNumber', t.IntegerType(), True),
                              ])
    df = read(spark_session, df_path, df_schema)
    return df


def read_movie_titles(spark_session):
    df_path = './imdb-data/title.akas.tsv.gz'
    df_schema = t.StructType([t.StructField('titleId', t.StringType(), False),
                              t.StructField('ordering', t.IntegerType(), False),
                              t.StructField('title', t.StringType(), True),
                              t.StructField('region', t.StringType(), True),
                              t.StructField('language', t.StringType(), True),
                              t.StructField('types', t.StringType(), True),
                              t.StructField('attributes', t.StringType(), True),
                              t.StructField('isOriginalTitle', t.IntegerType(), True),
                              ])
    df = read(spark_session, df_path, df_schema)
    return df


def transform(df1, df2):
    # get episodes
    df1 = df1.select('parentTconst', 'episodeNumber').where(col('episodeNumber').isNotNull())
    # get movie titles
    df2 = df2.select('titleId', 'title').where(col('types') == 'original')
    # join tables
    expanded_df = (df1.join(df2, col('parentTconst') == col('titleId'), 'left').select('title', 'episodeNumber')
                      .where(col('title').isNotNull()))
    # calculate episodes per movie
    window_tv_series = Window.partitionBy("title").orderBy(col("episodeNumber").desc())
    expanded_df = (expanded_df.withColumn("row", row_number().over(window_tv_series))
                   .withColumn("max", max(col('episodeNumber')).over(window_tv_series))
                   .select(col('title').alias('TV Series'), col('max').alias('Episodes'))
                   .filter(col("row") == 1).orderBy(col("episodeNumber").desc()).limit(50))
    return expanded_df


def task(spark_session):
    # get table of episodes
    episodes_df = read_episodes(spark_session)
    # get table of movie titles
    movie_titles_df = read_movie_titles(spark_session)
    # get and write result table
    write(transform(episodes_df, movie_titles_df), './imdb-data/task6')
