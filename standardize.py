# hdfs -> sparkSQL
import json
import logging

import pandas
import time
from io import TextIOWrapper
from typing import Any, Union

import pyspark
import requests
import wget as wget
from pyspark import RDD
from pyspark.python.pyspark.shell import spark
from pyspark.sql import DataFrame

from requests import Response

from serverLib import *

from hdfs import HDFS_handler
from hadoop_connect import hdfs_get_file
from serverLib import Constants


def save_json_as_spark_table(source: Union[RDD, list, pandas.DataFrame], tablename: str):
    # https://stackoverflow.com/questions/34196302/the-root-scratch-dir-tmp-hive-on-hdfs-should-be-writable-current-permissions
    print(source.columns, source.shape)
    print(source)
    # Creates a DataFrame from an RDD, a list or a pandas.DataFrame.
    df: DataFrame = Constants.SPARK_SESSION.createDataFrame(data=source, schema=None)
    df.printSchema()
    # df.cache()
#    df.write.option("path", '/temp').saveAsTable('connotations')
    try:
        df.write.saveAsTable(tablename)
    except pyspark.sql.utils.AnalysisException as tableExists:
        logging.warning(tableExists)
    # df.write.saveAsTable('connotations')
    # TODO: Constants.spark_session
    table: DataFrame = Constants.SPARK_SESSION.table(tableName=tablename)
    table.show()


def main():
    # Extract

    # btc_response: Union[dict, Response] = \
    #     hdfs_get_file(hadoop_path='crypto/tmp/crypto.json', params={"op": "OPEN", "noredirect": "true"})
    # # print(btc_response.json()['Location'])
    # response = requests.get(url=btc_response.json()['Location'])
    # print(response, response.url)
    # # print(response, type(response.json()))
    # # print(response.text)
    # # print(response, response.content)
    # #print(response.json().replace('\'', '\"'))
    # #print(type(json.loads(response.json().replace('\'', '\"'))))
    #
    # # print(type(response.json())['tsla'])

    # from data_types_and_structures import DataTypesHandler
    # DataTypesHandler.print_data_recursively(
    #     data=response.json(), print_dict=DataTypesHandler.PRINT_DICT
    # )

    # wget.download(
    #     btc_response.json()['Location'],
    #     r'C:\Users\adam l\Desktop\python files\BigData\BD_projects\cryptocorrencies\server_simple\tmp\file.json'
    # )

    stocks_list: Response = hdfs_get_file(
        hadoop_path='crypto/stocks', params={'op': 'LISTSTATUS'}
    )

    # from data_types_and_structures import DataTypesHandler
    # DataTypesHandler.print_data_recursively(
    #     data=stocks_list.json(), print_dict=DataTypesHandler.PRINT_DICT
    # )

    stock_names = []

    for file in stocks_list.json()['FileStatuses']['FileStatus']:
        print(file['pathSuffix'])
        stock_names.append(file['pathSuffix'].split('.')[0])

    for stockname in stock_names:
        try:
            df: DataFrame = Constants.SPARK_SESSION.read.json(
                path=f'{HDFS_handler.DEFAULT_CLUSTER_PATH}crypto/stocks/{stockname}.json',
                schema=None)  # core-site.xml
            df.show()
            df.printSchema()
            print(df.select(f'{stockname}.status.values').collect())

            # return
            # Transform
            # Constants.SPARK_SESSION.table('crypto').select('"tsla.description"').show()
            try:
                df.write.saveAsTable(stockname)
            except pyspark.sql.utils.AnalysisException as tableExists:
                logging.warning(tableExists)
                # df.write.saveAsTable('connotations')
                # TODO: Constants.spark_session
            table: DataFrame = Constants.SPARK_SESSION.table(tableName=stockname)
            table.show()
            print(table.select(f'`{stockname}`.status.`keys`').collect())

            # spark.sql('SELECT spark_catalog.default.crypto.tsla.status.values FROM crypto').show()
            # table.select('spark_catalog.default.crypto.tsla.status.values').show()
            print(table.columns)
            # Constants.SPARK_SESSION.sql(
            #    f'SELECT `{stockname}.status.values` AS values, `{stockname}.status.keys` AS keys, '
            #    f'`{stockname}.description` AS description '
            #    f'FROM {stockname}'
            # ).show()
            table.createOrReplaceTempView(stockname)
            Constants.SPARK_SESSION.sql(f'select `{stockname}`.status.keys from {stockname}').show()

            spark.sql('SHOW DATABASES').show()
            spark.sql('SHOW SCHEMAS').show()
            spark.sql('SHOW TABLES').show()
            spark.sql('CREATE DATABASE IF NOT EXISTS stocks COMMENT "For stocks & cryptocurrencies"')
            spark.sql('USE stocks')

            spark.sql('DESCRIBE DATABASE EXTENDED stocks').show()

            try:
                spark.sql(
                   f'SELECT `{stockname}`.status.`values` AS values, `{stockname}`.status.keys AS keys, '
                   f'`{stockname}`.description AS description '
                   f'FROM default.{stockname}'
                ).write.saveAsTable(stockname)
            except pyspark.sql.utils.AnalysisException as tableExists:
                logging.warning(tableExists)
            spark.sql('SHOW TABLES').show()

            spark.sql(f'SELECT * FROM stocks.{stockname}').show()
            spark.sql(f'SELECT * FROM stocks.{stockname}').printSchema()

            print(spark.sql(f'SELECT {stockname}.status.values FROM stocks.{stockname}').collect())

            df.createOrReplaceTempView(f'{stockname}_temp')
            tesla_temp: DataFrame = spark.sql(
                f'SELECT {stockname}.status.values AS values, {stockname}.status.keys AS keys, '
                f'{stockname}.description AS description '
                f'FROM {stockname}_temp'
            )
            tesla_temp.show()
            print(tesla_temp.select('values').collect())
            spark.table(f'stocks.{stockname}').drop()
            spark.sql(f'DROP TABLE stocks.{stockname}')
            try:
                tesla_temp.write.saveAsTable(f'stocks.{stockname}')
            except pyspark.sql.utils.AnalysisException as tableExists:
                logging.warning(tableExists)
            spark.table(f'stocks.{stockname}').show()
            print(spark.table(f'stocks.{stockname}').select('values').collect())
            # Load
            # # pandas.DataFrame.from_dict()
            # save_json_as_spark_table(source=pandas.json_normalize(response.json()), tablename='crypto')
            spark.sql(f'DROP TABLE stocks.{stockname}')
            spark.sql(f'DROP TABLE stocks.{stockname}_temp')

        except Exception as e:
            print(f'stock {stockname} failed')
            print(e)
            # raise e

        spark.sql('SHOW TABLES').show()


if __name__ == '__main__':
    main()
