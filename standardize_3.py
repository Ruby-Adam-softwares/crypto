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


# def process_file(process_data_fn, file: str, format: str) -> Any:
#     st = time.time()
#
#     from hdfs import HDFS_handler
#
#     HDFS_handler.start()
#     HDFS_handler.safemode_off()
#
#     ret_processed: Any = pass_file_to_spark(
#         file_path=f"{HDFS_handler.DEFAULT_CLUSTER_PATH}{HDFS_handler.HADOOP_USER}/{file}",
#         process_fn=process_data_fn, format=format
#     )
#
#     HDFS_handler.safemode_on()
#     HDFS_handler.stop()
#
#     logging.debug(f"Spark Time: {time.time() - st} seconds ({(time.time() - st)//60} minutes)")
#     return ret_processed
#
#
# def pass_file_to_spark(process_fn, file_path: str, format: str, **kwargs) -> Any:
#     """
#     Passes a given file to spark and processes it with a given function
#     :param process_fn :type function(data_frame: DataFrame) -> the function that processes the given file
#     :param file_path :type str: the path of the file that should be processed
#     :return :type dict:
#     """
#     from hdfs import HDFS_handler
#     from py4j.protocol import Py4JJavaError
#     try:
#         df: DataFrame = spark.read.load(path=file_path, format=format, **kwargs)
#         # df.show()
#         return process_fn(df)
#         # return process_fn(data_frame=df)
#
#     except Py4JJavaError as e:
#         logging.debug(type(e.java_exception))
#         if "java.net.ConnectException" in e.java_exception.__str__():
#             logging.error("HDFS cluster is down")
#         else:
#             HDFS_handler.stop()
#             raise
#     except:
#         HDFS_handler.stop()
#         raise


def save_json_as_spark_table(source: Union[RDD, list, pandas.DataFrame], tablename: str):
    # https://stackoverflow.com/questions/34196302/the-root-scratch-dir-tmp-hive-on-hdfs-should-be-writable-current-permissions
    print(source.columns, source.shape)
    print(source)
    # Creates a DataFrame from an RDD, a list or a pandas.DataFrame.
    df: DataFrame = Constants.SPARK_SESSION.createDataFrame(data=source, schema=None)
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
    btc_response: Union[dict, Response] = \
        hdfs_get_file(hadoop_path='crypto/tmp/crypto.json', params={"op": "OPEN", "noredirect": "true"})
    # print(btc_response.json()['Location'])
    response = requests.get(url=btc_response.json()['Location'])
    print(response, response.url)
    # print(response, type(response.json()))
    # print(response.text)
    # print(response, response.content)
    #print(response.json().replace('\'', '\"'))
    #print(type(json.loads(response.json().replace('\'', '\"'))))

    # print(type(response.json())['tsla'])
    from data_types_and_structures import DataTypesHandler
    DataTypesHandler.print_data_recursively(
        data=response.json(), print_dict=DataTypesHandler.PRINT_DICT
    )


    # wget.download(
    #     btc_response.json()['Location'],
    #     r'C:\Users\adam l\Desktop\python files\BigData\BD_projects\cryptocorrencies\server_simple\tmp\file.json'
    # )

    # return
    # Transform
    # Load
    # pandas.DataFrame.from_dict()
    save_json_as_spark_table(source=pandas.json_normalize(response.json()), tablename='crypto')


if __name__ == '__main__':
    main()
