"""
   Get titles of all movies that last more than 2 hours
"""

import pyspark.sql.types as t
from pyspark.sql.functions import col
from read_write import read, write


def read_titles1(spark_session):
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


def read_titles2(spark_session):
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


def transform(titles_df1, titles_df2):
    # get movie durations
    titles_df1 = (titles_df1.withColumn('runtimeHours', col('runtimeMinutes') / 60)
                            .select('tconst', 'runtimeMinutes', 'runtimeHours')
                            .where(col('runtimeHours') > 2))
    # get movie names
    titles_df2 = (titles_df2.select('titleId', 'title', 'isOriginalTitle')
                            .where(col('isOriginalTitle') == 1))
    # get result table
    expanded_df = (titles_df1.join(titles_df2, col('tconst') == col('titleId'), 'left')
                             .where(col('title').isNotNull()))
    expanded_df = (expanded_df.select(col('title').alias('title_original'),
                                      col('runtimeMinutes').alias('duration_in_minutes')))
    return expanded_df


def task(spark_session):
    # get table of movie durations
    titles_df1 = read_titles1(spark_session)
    # get table of movie names
    titles_df2 = read_titles2(spark_session)
    # get and write result table
    write(transform(titles_df1, titles_df2), './imdb-data/task3')
