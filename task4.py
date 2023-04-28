"""
   Get names of people, corresponding movies/series and characters they played in those films.
"""

import pyspark.sql.types as t
from pyspark.sql.functions import col
from read_write import read, write


def read_movies_people(spark_session):
    df_path = './imdb-data/title.principals.tsv.gz'
    df_schema = t.StructType([t.StructField('tconst', t.StringType(), False),
                              t.StructField('ordering', t.IntegerType(), False),
                              t.StructField('nconst', t.StringType(), False),
                              t.StructField('category', t.StringType(), True),
                              t.StructField('job', t.StringType(), True),
                              t.StructField('characters', t.StringType(), True),
                              ])
    df = read(spark_session, df_path, df_schema)
    return df


def read_people_names(spark_session):
    df_path = './imdb-data/name.basics.tsv.gz'
    df_schema = t.StructType([t.StructField('nconst', t.StringType(), False),
                              t.StructField('primaryName', t.StringType(), False),
                              t.StructField('birthYear', t.IntegerType(), True),
                              t.StructField('deathYear', t.IntegerType(), True),
                              t.StructField('primaryProfession', t.StringType(), True),
                              t.StructField('knownForTitles', t.StringType(), True),
                              ])
    df = read(spark_session, df_path, df_schema)
    return df


def read_movies_titles(spark_session):
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


def transform(movies_people_df, people_df, movies_df):
    # get matching of people and movies
    movies_people_df = (movies_people_df.select('tconst', 'nconst', 'characters')
                                        .where(col('category').isin('actor', 'actress')))
    # get people names
    people_df = people_df.select('nconst', 'primaryName')
    # get movie names
    movies_df = movies_df.select('titleId', 'title').where((col('isOriginalTitle') == 1))
    # get result table
    expanded_df = (movies_people_df.join(people_df, on='nconst', how='left')
                                   .join(movies_df, col('tconst') == col('titleId'), how='left')
                                   .select('primaryName', 'title', 'characters')
                                   .where(col('title').isNotNull()))
    return expanded_df


def task(spark_session):
    # get table of people in movies
    movies_people_df = read_movies_people(spark_session)
    # get table of people
    people_df = read_people_names(spark_session)
    # get table of movies
    movies_df = read_movies_titles(spark_session)
    # get and write results
    write(transform(movies_people_df, people_df, movies_df), './imdb-data/task4')
