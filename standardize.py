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

    df: DataFrame = Constants.SPARK_SESSION.read.json(
        path=f'{HDFS_handler.DEFAULT_CLUSTER_PATH}crypto/tmp/crypto.json',
        schema=None)  # core-site.xml
    df.show()
    df.printSchema()

    # return
    # Transform
    try:
        df.write.saveAsTable('crypto')
    except pyspark.sql.utils.AnalysisException as tableExists:
        logging.warning(tableExists)
        # df.write.saveAsTable('connotations')
        # TODO: Constants.spark_session
    table: DataFrame = Constants.SPARK_SESSION.table(tableName='crypto')
    table.show()
    # spark.sql('SELECT spark_catalog.default.crypto.tsla.status.values FROM crypto').show()
    # table.select('spark_catalog.default.crypto.tsla.status.values').show()
    print(table.columns)
    Constants.SPARK_SESSION.sql(
       'SELECT `tsla.status.values` AS values, `tsla.status.keys` AS keys, `tsla.description` AS description '
       'FROM crypto'
    ).show()
    table.createOrReplaceTempView('tesla')
    Constants.SPARK_SESSION.sql('select `tsla.status.keys` from tesla').show()

    # Constants.SPARK_SESSION.table('crypto').select('"tsla.description"').show()
    # Load
    # # pandas.DataFrame.from_dict()
    # save_json_as_spark_table(source=pandas.json_normalize(response.json()), tablename='crypto')


if __name__ == '__main__':
    main()
