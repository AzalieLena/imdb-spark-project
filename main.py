from pyspark import SparkConf
from pyspark.sql import SparkSession
import task1 as t1
import task2 as t2
import task3 as t3
import task4 as t4
import task5 as t5
import task6 as t6
import task7 as t7
import task8 as t8


def main():
    spark_session = (SparkSession.builder
                     .master("local")
                     .appName("imdb-spark-project")
                     .config(conf=SparkConf())
                     .getOrCreate())
    t1.task(spark_session)
    t2.task(spark_session)
    t3.task(spark_session)
    t4.task(spark_session)
    t5.task(spark_session)
    t6.task(spark_session)
    t7.task(spark_session)
    t8.task(spark_session)


if __name__ == "__main__":
    main()
