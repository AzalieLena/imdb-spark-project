"""
   Get the list of people’s names, who were born in the 19th century
"""

import pyspark.sql.types as t
from pyspark.sql.functions import col
from read_write import read, write


def read_people(spark_session):
    df_path = './imdb-data/name.basics.tsv.gz'
    df_schema = t.StructType([t.StructField('nconst', t.StringType(), False),
                              t.StructField('primaryName', t.StringType(), False),
                              t.StructField('birthYear', t.IntegerType(), True),
                              t.StructField('deathYear', t.IntegerType(), True),
                              t.StructField('primaryProfession', t.StringType(), False),
                              t.StructField('knownForTitles', t.StringType(), False)
                              ])
    df = read(spark_session, df_path, df_schema)
    return df


def transform(people_df):
    # get people names and birthdate
    people_df = (people_df.select(col('primaryName').alias('name'), col('birthYear').alias('birth_year'))
                          .where((col('birth_year') > 1800) & (col('birth_year') <= 1900)))
    return people_df


def task(spark_session):
    # get table of people
    people_df = read_people(spark_session)
    # get and write result table
    write(transform(people_df), './imdb-data/task2')
