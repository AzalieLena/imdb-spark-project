import pyspark.sql.functions as f

def read(spark_session, path, schema):
    df = spark_session.read.csv(path,
                                        header=True,
                                        sep='\t',
                                        nullValue='null',
                                        schema=schema
                                        )
    for _, name_col in enumerate(df.columns):
        df = df.withColumn(name_col, f.when(f.col(name_col) == '\\N', None).otherwise(f.col(name_col)))
    return df


def write(df, directory_to_write):
    df.write.csv(directory_to_write,
                 header=True,
                 sep='\t'
                 )
    return
