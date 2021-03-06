# -*- coding: utf-8 -*-
import platform
from datetime import datetime, timedelta
import logging
import time

# https://stackoverflow.com/questions/22715086/scheduling-python-script-to-run-every-hour-accurately
# https://data.gov.il/dataset/covid-19  # מאגר קורונה
# https://data.gov.il/dataset/covid-19/resource/8a21d39d-91e3-40db-aca1-f73f7ab1df69  # טבלת יישובים
# https://data.gov.il/dataset/covid-19/resource/0995c344-6a7a-4557-99ff-28ee6f3149b3  # טבלת יישובים README
# https://data.gov.il/dataset/covid-19/resource/89f61e3a-4866-4bbf-bcc1-9734e5fee58e  # קבוצות מין וגיל
# https://console.firebase.google.com/u/2/project/corona-charts-33e8a/database/corona-charts-33e8a-default-rtdb/data/~2F
# https://stackoverflow.com/questions/30483977/python-get-yesterdays-date-as-a-string-in-yyyy-mm-dd-format/30484112

from playsound import playsound
import sys

from pyspark.python.pyspark.shell import spark

sys.path.append(".")
from serverLib import *
from hdfs import HDFS_handler


def start_server():
    HDFS_handler.start()

    run_py_file("get_save_data")
    run_py_file("standardize")
    run_py_file("process")
    run_py_file("analyze")
    run_py_file("make_data_accessible")

    HDFS_handler.stop()


@Constants.SCHEDULER.scheduled_job('cron', day_of_week='mon-sun', hour=7, minute=0, second=0)
def scheduled_job():
    print('This job is run every weekday at 7am.')
    firebase_config()
    start_server()
    print(datetime.now())

    play_sound()

    logging.debug(f"Program Total Time: {time.time() - st} seconds")
    logging.debug(f"Program Total Time: {(time.time() - st)//60} minutes")


def main():
    # kobi@kobi-A1SAi:~/Adam_desk/BigData/BigData-master/BD_projects$ python3 Ruby_corona_charts/python/linuxServer.py
    st = time.time()

    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('flaskwebgui').setLevel(logging.ERROR)
    logging.getLogger('BaseHTTPRequestHandler').setLevel(logging.ERROR)
    logging.getLogger('matplotlib').setLevel(logging.ERROR)
    logging.getLogger('py4j').setLevel(logging.ERROR)
    logging.getLogger('my_log').setLevel(logging.DEBUG)
    logging.getLogger('HiveConf').setLevel(logging.ERROR)

    spark.sparkContext.setLogLevel('ERROR')
    Constants.SPARK_SESSION.sparkContext.setLogLevel('ERROR')

    def set_scheduler():
        play_sound()
        inp = input("Do you want to start a scheduled server? [y/n]\n")
        if inp == "y" or inp == "Y":
            Constants.RUN_SCHEDULER = True
        elif inp == "n" or inp == "N":
            Constants.RUN_SCHEDULER = False
        else:
            print("Syntax Error. Please enter a valid answer")
            set_scheduler()
    set_scheduler()

    try:
        firebase_config()
        print(datetime.now())
        set_current_os(os_type=platform.system())

        if Constants.RUN_SCHEDULER:
            # pass
            start_server()
            play_sound()
            Constants.SCHEDULER.start()
        else:
            start_server()
    finally:
        play_sound()

        logging.debug(f"Program Total Time: {time.time() - st} seconds")
        logging.debug(f"Program Total Time: {(time.time() - st)//60} minutes")


if __name__ == '__main__':
    main()
