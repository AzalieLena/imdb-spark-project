"""
   Get information about how many adult movies/series etc. there are per region.
   Get the top 100 of them from the region with the biggest count to the region with the smallest one
"""

import pyspark.sql.types as t
from pyspark.sql.functions import col
from read_write import read, write


def read_movies1(spark_session):
    df_path = './imdb-data/title.basics.tsv.gz'
    df_schema = t.StructType([t.StructField('tconst', t.StringType(), False),
                              t.StructField('titleType', t.StringType(), False),
                              t.StructField('primaryTitle', t.IntegerType(), True),
                              t.StructField('originalTitle', t.IntegerType(), True),
                              t.StructField('isAdult', t.IntegerType(), True),
                              t.StructField('startYear', t.IntegerType(), True),
                              t.StructField('endYear', t.IntegerType(), True),
                              t.StructField('runtimeMinutes', t.IntegerType(), True),
                              t.StructField('genres', t.StringType(), True),
                              ])
    df = read(spark_session, df_path, df_schema)
    return df


def read_movies2(spark_session):
    df_path = './imdb-data/title.akas.tsv.gz'
    df_schema = t.StructType([t.StructField('titleId', t.StringType(), False),
                              t.StructField('ordering', t.IntegerType(), False),
                              t.StructField('title', t.StringType(), True),
                              t.StructField('region', t.StringType(), True),
                              t.StructField('language', t.StringType(), True),
                              t.StructField('types', t.StringType(), True),
                              t.StructField('attributes', t.StringType(), True),
                              t.StructField('isOriginalTitle', t.StringType(), True),
                              ])
    df = read(spark_session, df_path, df_schema)
    return df


def transform(adult_movies_df, movie_regions_df):
    # get movies for adults
    adult_movies_df = adult_movies_df.select('tconst', 'isAdult').where(col('isAdult') == 1)
    # get movie regions
    movie_regions_df = movie_regions_df.select('titleId', 'region')
    # get result table
    expanded_df = (adult_movies_df.join(movie_regions_df, col('tconst') == col('titleId'), 'left')
                                  .select('region').where(col('region').isNotNull())
                                  .groupBy('region').count().orderBy('count', ascending=False).limit(100))
    return expanded_df


def task(spark_session):
    # get table of movies with adult status
    adult_movies_df = read_movies1(spark_session)
    # get table of movies with regions
    movie_regions_df = read_movies2(spark_session)
    # get and write result table
    write(transform(adult_movies_df, movie_regions_df), './imdb-data/task5')
