# sparkML -> firebase
import json
import logging

from pyspark.shell import spark
from pyspark.sql import DataFrame
from serverLib import *
from hadoop_connect import hdfs_get_file

from serverLib import Constants, firebase_config


def get_spark_table(name: str) -> DataFrame:
    table: DataFrame = Constants.SPARK_SESSION.table(tableName=name)
    table.printSchema()
    return table


def spark_table_to_json(table: DataFrame) -> dict:
    json_res: dict = table.toPandas().to_dict()
    print(json_res.keys())
    ret = {}
    for key in json_res.keys():
        print(type(key))

    # toJSON() turns each row of the DataFrame into a JSON string
    # calling first() on the result will fetch the first row.
    results: dict = json.loads(table.toJSON().first())
    rows: list = table.toJSON().collect()
    print(rows)
    # for item in rows:
    #     results: dict = json.loads(item)
    print(type(results))
    return results


def upload_to_firebase(json_update: dict):
    # print(json)
    from data_types_and_structures import DataTypesHandler
    DataTypesHandler.print_data_recursively(
        data=json_update, print_dict=DataTypesHandler.PRINT_DICT
    )

    firebase_config()
    # print(firebase.database().child('test').get().val())
    Constants.db.update(json_update)
    print(Constants.db.get().val())


def get_all_data():
    # spark.sql('SHOW TABLES').createOrReplaceTempView('all_tables')
    # print(spark.table('all_tables').select('tableName FROM all_tables WHERE isTemporary = false').collect())
    # spark.sql(f'DROP TABLE all_tables')
    spark.sql('CREATE DATABASE IF NOT EXISTS stocks COMMENT "For stocks & cryptocurrencies"')
    spark.sql('USE stocks')

    spark.sql('SHOW TABLES').select('tableName').filter('isTemporary = false').show()
    # print(spark.sql('SHOW TABLES').select('tableName').filter('isTemporary = false').collect())
    # print(type(spark.sql('SHOW TABLES').select('tableName').filter('isTemporary = false').collect()))

    tables = []
    for table in spark.sql('SHOW TABLES').select('tableName').filter('isTemporary = false').collect():
        tables.append(table['tableName'])

    all_data = {}
    for tab in tables:
        all_data[tab] = json.loads(spark.table(f'stocks.{tab}').toJSON().first())

    from data_types_and_structures import DataTypesHandler
    DataTypesHandler.print_data_recursively(
        data=all_data, print_dict=DataTypesHandler.PRINT_DICT
    )


def main():
    get_all_data()
    # Extract
    # hdfs_get_file(hadoop_path='crypto/tmp/crypto.json', params={"op": "OPEN"})
    spark.sql('USE default')

    table: DataFrame = get_spark_table('crypto')
    # Transform
    json_send: dict = spark_table_to_json(table=table)

    ret: dict = {}
    ret['keys'] = json_send['tsla.status.keys']
    ret['values'] = json_send['tsla.status.values']
    ret['description'] = json_send['tsla.description']
    # Load
    upload_to_firebase(json_update={'test': json.loads(spark.table('stocks.tesla').toJSON().first())})

    # pass


if __name__ == '__main__':
    main()
