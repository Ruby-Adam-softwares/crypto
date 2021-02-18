import logging
import time
from typing import Any, Union

import requests
from requests import Response


def update_file_to_hdfs(hadoop_path: str, data: Any):  # local_file_path: str,
    connect_to_hdfs(hdfs_url=fr'http://localhost:9870/webhdfs/v1/{hadoop_path}', data=data)

    # HDFS_handler.ls('user/hduser/connotation')
    # HDFS_handler.list_files()


def connect_to_hdfs(hdfs_url: str, data: Any):
    # https://stackoverflow.com/questions/48735869/error-failed-to-retrieve-data-from-webhdfs-v1-op-liststatus-server-error-wh/59112061#59112061
    # https://hadoop.apache.org/docs/r3.1.1/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#List_a_Directory

    # http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=LISTSTATUS
    # response = requests.get(url='http://localhost:9870/webhdfs/v1/user',
    #              params={"op": "LISTSTATUS"})
    from hdfs import HDFS_handler
    HDFS_handler.safemode_off()

    # TODO:
    # if response[RemoteException][exception] == 'SafeModeException':
    #   HDFS_handler.safemode_off()
    #   time.sleep(20)
    #   send_request()

    # TODO:
    # if response[RemoteException][exception] == 'IOException':
    #   HDFS_handler.safemode_off()
    #   time.sleep(20)
    #   send_request()
    # TODO: SafeModeException, IOException, FileNotFoundException

    time.sleep(2)
    # hdfs_url = 'http://localhost:9870/webhdfs/v1/test/wiki.json'
    # hdfs_url = 'http://localhost:9870/webhdfs/v1/test/wiki.json'

    def hdfs_put_request(url: str, params: dict, data: Any):
        response = requests.put(
            url=url,
            params=params,
            data=data
            # data='{"test1": "test2", "test tast im testing": 2}'
            # data = {"test1": "test2", "test tast im testing": 2}
        )
        try:
            if response.json()['RemoteException']['exception'] == 'SafeModeException':
                HDFS_handler.safemode_off()
                time.sleep(10)
                hdfs_put_request(url, params, data)
                return
            elif response.json()['RemoteException']['exception'] == 'IOException':
                logging.error('Cluster error:')
                logging.error(response.json())
                time.sleep(10)
                hdfs_put_request(url, params, data)
                return
            elif response.json()['RemoteException']['exception'] == 'FileNotFoundException':
                logging.error("Cannot find the required HDFS file:")
                logging.error(response.json())
                return
            else:
                print(response.json())
                from data_types_and_structures import DataTypesHandler
                DataTypesHandler.print_data_recursively(
                    data=response.json(), print_dict=DataTypesHandler.PRINT_DICT
                )
        except:
            print(response, response.url)
            print(response.text)

    hdfs_put_request(
        url=hdfs_url,
        params={
                "op": "CREATE", "overwrite": "true", 'user.name': 'livne'
            },
        data=data
    )

    time.sleep(2)

    def hdfs_get_request(url: str, params: dict):
        response = requests.get(
            url=url,
            params=params
        )
        try:
            if response.json()['RemoteException']['exception'] == 'SafeModeException':
                HDFS_handler.safemode_off()
                time.sleep(10)
                hdfs_get_request(url, params)
                return
            elif response.json()['RemoteException']['exception'] == 'IOException':
                logging.error('Cluster error:')
                logging.error(response.json())
                time.sleep(10)
                hdfs_get_request(url, params)
                return
            elif response.json()['RemoteException']['exception'] == 'FileNotFoundException':
                logging.error("Cannot find the required HDFS file:")
                logging.error(response.json())
                return
            else:
                print(response.json())
                from data_types_and_structures import DataTypesHandler
                DataTypesHandler.print_data_recursively(
                    data=response.json(), print_dict=DataTypesHandler.PRINT_DICT
                )
        except:
            print(response, response.url)
            print(response.text)

    hdfs_get_request(url=hdfs_url, params={"op": "OPEN"})
    # hdfs dfs -get /connotation/testspeech.mp4 'C:\Users\adam l\Desktop\test'

    HDFS_handler.safemode_on()


def hdfs_get_file(hadoop_path: str, params: dict) -> Union[dict, Response]:
    url = fr'http://localhost:9870/webhdfs/v1/{hadoop_path}'
    response = requests.get(
        url=url,
        params=params
    )
    try:
        print(response.json())
        if response.json()['RemoteException']['exception'] == 'SafeModeException':
            HDFS_handler.safemode_off()
            time.sleep(10)
            return hdfs_get_request(url, params)
        elif response.json()['RemoteException']['exception'] == 'IOException':
            logging.error('Cluster error:')
            logging.error(response.json())
            time.sleep(10)
            return hdfs_get_request(url, params)

        elif response.json()['RemoteException']['exception'] == 'FileNotFoundException':
            logging.error("Cannot find the required HDFS file:")
            logging.error(response.json())
            return
        else:
            print(response.json())
            from data_types_and_structures import DataTypesHandler
            DataTypesHandler.print_data_recursively(
                data=response.json(), print_dict=DataTypesHandler.PRINT_DICT
            )
            return response.json()
    except:
        print(response, response.url)
        print(response.text)
        return response

