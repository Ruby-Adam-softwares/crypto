# data source -> hdfs
import json
import pandas

from serverLib import *
from hadoop_connect import *
from api import *

from hdfs import HDFS_handler
from hadoop_connect import update_file_to_hdfs
from api import get_stock


def main():
    # TODO: add params support
    # HDFS_handler.start()

    # Extract
    # Transform
    # Load
    # print(get_stock())
    #stock: dict = get_stock()
    #print(stock)
    update_file_to_hdfs(hadoop_path='crypto/tmp/crypto.json', data=json.dumps(get_stock()))
    #firebase_config()
    #Constants.db.update(stock)
    #print(Constants.db.get().val())

    # HDFS_handler.stop()


if __name__ == '__main__':
    main()
