import logging

import pyspark
from pyspark import Row
from pyspark.python.pyspark.shell import spark
from pyspark.sql import DataFrame, SparkSession

from hdfs import HDFS_handler


def main():
    spark_session: SparkSession = SparkSession \
        .builder \
        .appName("StructuredNetworkWordCount") \
        .enableHiveSupport() \
        .config("spark.sql.streaming.schemaInference", True) \
        .getOrCreate()

    df: DataFrame = spark_session.read.json(
        path=f'{HDFS_handler.DEFAULT_CLUSTER_PATH}crypto/tmp/crypto.json',
        schema=None)  # core-site.xml
    df.show()
    df.printSchema()
    df.createOrReplaceTempView('tesla')
    df.drop()
    spark_session.sql('select tsla.status.keys from tesla').show()
    print(spark_session.sql('select tsla.status.values from tesla').collect()[0].asDict()['values'])
    print(spark_session.sql('select tsla.status.values from tesla').collect()[0][0])
    spark_session.sql('SELECT * FROM tesla').summary().show()
    spark_session.table('tesla').describe().show()
    spark_session.table('tesla').summary().printSchema()
    print(spark.table('tesla').select('tsla.status.values').summary().collect())

    spark_session.sql(
        'SELECT tsla.status.values AS values, tsla.status.keys AS keys, tsla.description AS description FROM tesla'
    ).show()

    keys: list = spark_session.sql('SELECT tsla.status.keys FROM tesla').collect().pop().asDict()['keys']
    values: list = spark_session.sql('SELECT tsla.status.values FROM tesla').collect().pop().asDict()['values']

    spark_session.sql(
        'CREATE TABLE IF NOT EXISTS tes '
        'USING JSON '
        'AS SELECT tsla.status.values AS values, tsla.status.keys AS keys, tsla.description AS description '
        'FROM tesla'
    )

    spark_session.table('tes2').drop()
    spark_session.sql(
        'CREATE TABLE IF NOT EXISTS tes2(key STRING, value INT) '
        'USING JSON '
    )
    spark_session.table('tes2').printSchema()
    for key in keys:
        try:
            # print(float(values[keys.index(key)]))
            if float(values[keys.index(key)]):
                print(f'{key}, {values[keys.index(key)]}')
                spark_session.sql(f'INSERT INTO tes2 VALUES("{str(key)}", {values[keys.index(key)]})')
        except ValueError as e:
            logging.error(e)
    # spark_session.sql(f'INSERT INTO tes2 VALUES("Currency", "{values[keys.index("Currency")]}")')
    spark_session.table('tes2').distinct().show()
    # spark_session.table('tes2').dropDuplicates('key').show()

    try:  # works as a db
        spark_session.table('tes2').write.saveAsTable('tes_persistent')  # persistent table
    except pyspark.sql.utils.AnalysisException as tableExists:
        logging.warning(tableExists)

    # TODO: Constants.spark_session
    # table: DataFrame = Constants.SPARK_SESSION.table(tableName=tablename)
    # table.show()
    spark_session.table('tes_persistent').show()


if __name__ == '__main__':
    main()
