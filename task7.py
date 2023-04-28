"""
   Get 10 titles of the most popular movies/series etc. by each decade.
"""

import pyspark.sql.types as t
from pyspark.sql.functions import col, row_number, ceil
from read_write import read, write
from pyspark.sql.window import Window


def read_movie_titles1(spark_session):
    df_path = './imdb-data/title.basics.tsv.gz'
    df_schema = t.StructType([t.StructField('tconst', t.StringType(), False),
                              t.StructField('titleType', t.StringType(), False),
                              t.StructField('primaryTitle', t.IntegerType(), True),
                              t.StructField('originalTitle', t.IntegerType(), True),
                              t.StructField('isAdult', t.BooleanType(), True),
                              t.StructField('startYear', t.IntegerType(), True),
                              t.StructField('endYear', t.IntegerType(), True),
                              t.StructField('runtimeMinutes', t.IntegerType(), True),
                              t.StructField('genres', t.StringType(), True),
                              ])
    df = read(spark_session, df_path, df_schema)
    return df


def read_movie_titles2(spark_session):
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


def read_movie_ratings(spark_session):
    df_path = './imdb-data/title.ratings.tsv.gz'
    df_schema = t.StructType([t.StructField('tconst', t.StringType(), False),
                              t.StructField('averageRating', t.DoubleType(), True),
                              t.StructField('numVotes', t.IntegerType(), True),
                              ])
    df = read(spark_session, df_path, df_schema)
    return df


def transform(movie_titles_df1, movie_titles_df2, movie_ratings_df):
    # get movie titles and starting years
    movie_titles_df1 = movie_titles_df1.select('tconst', 'startYear')
    movie_titles_df2 = (movie_titles_df2.select('titleId', 'title')
                        .where(col('isOriginalTitle') == 1))
    # join title tables
    movie_titles_df = (movie_titles_df1.join(movie_titles_df2, col('tconst') == col('titleId'), 'left')
                       .select('tconst', 'title', 'startYear'))
    # get movie ratings
    movie_ratings_df = movie_ratings_df.select('tconst', 'averageRating')
    # join with rating table
    expanded_df = (movie_titles_df.join(movie_ratings_df, on='tconst', how='left')
                   .select('title', 'averageRating', 'startYear')
                   .where(col('startYear').isNotNull() & col('averageRating').isNotNull() & col('title').isNotNull())
                   .withColumn('startDecade', 10 * ceil(col('startYear') / 10) - 9))
    # calculate movies per decade
    window_genre = Window.partitionBy('startDecade').orderBy(col('averageRating').desc())
    expanded_df = (expanded_df.withColumn("row", row_number().over(window_genre)).filter(col("row") <= 10)
                   .select('startDecade', 'title', 'averageRating')
                   .orderBy(col('startDecade').cast('string').asc(), col('averageRating').desc()))
    return expanded_df


def task(spark_session):
    # get tables of movie titles
    movie_titles_df1 = read_movie_titles1(spark_session)
    movie_titles_df2 = read_movie_titles2(spark_session)
    # get table of movie ratings
    movie_ratings_df = read_movie_ratings(spark_session)
    # get and write result table
    write(transform(movie_titles_df1, movie_titles_df2, movie_ratings_df), './imdb-data/task7')
