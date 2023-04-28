"""
   Get all titles of series/movies etc. that are available in Ukrainian
"""

import pyspark.sql.types as t
from pyspark.sql.functions import col
from read_write import read, write


def read_titles(spark_session):
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


def transform(titles_df):
    # get movie title_in_Ukrainian
    titles_df = titles_df.select(col('title').alias('title_in_Ukrainian')).where(col('region') == 'UA')
    return titles_df


def task(spark_session):
    # get table of movie titles
    titles_df = read_titles(spark_session)
    # get and write result table
    write(transform(titles_df), './imdb-data/task1')
