# data source -> hdfs
import json
import pandas
from requests import Response

from serverLib import *
from hadoop_connect import *
from api import *

from hdfs import HDFS_handler
from hadoop_connect import update_file_to_hdfs, hdfs_get_file
from api import get_stock, get_stock_by_symbol


def main():
    # TODO: add params support
    # HDFS_handler.start()

    # Extract
    # Transform
    # Load
    # print(get_stock())
    #stock: dict = get_stock()
    #print(stock)
    # update_file_to_hdfs(hadoop_path='crypto/tmp/crypto.json', data=json.dumps(get_stock()))
    # update_file_to_hdfs(hadoop_path='crypto/tmp/crypto.json',
    #                     data=json.dumps(get_stock_by_symbol(symbol='tsla', country='United States')))
    #
    # update_file_to_hdfs(hadoop_path='crypto/tmp/crypto.json',
    #                     data=json.dumps(get_stock_by_symbol(symbol='amzn', country='United States')))

    stock_list = [
        {'symbol': 'TSLA', 'country': 'United States'},
        {'symbol': 'AMZN', 'country': 'United States'},
        {'symbol': 'MSFT', 'country': 'United States'},
        {'symbol': 'AAPL', 'country': 'United States'},
        {'symbol': 'F', 'country': 'United States'},
        {'symbol': 'FB', 'country': 'United States'},
        {'symbol': 'GOOG', 'country': 'United States'},
    ]

    for stock in stock_list:
        update_file_to_hdfs(hadoop_path=f'crypto/stocks/{stock["symbol"]}.json',
                            data=json.dumps(get_stock_by_symbol(symbol=stock["symbol"], country=stock["country"])))

    #firebase_config()
    #Constants.db.update(stock)
    #print(Constants.db.get().val())

    # HDFS_handler.stop()


if __name__ == '__main__':
    main()
