# sparkSQL -> sparkML
from pyspark.sql import DataFrame

from serverLib import *

from serverLib import Constants


def get_spark_table(name: str) -> DataFrame:
    table: DataFrame = Constants.SPARK_SESSION.table(tableName=name)
    return table


def analyze(table: DataFrame):
    table.show()


def save_as_spark_table():
    pass


def main():
    # Extract
    table: DataFrame = get_spark_table('connotations')
    # Transform
    analyze(table)
    # Load
    save_as_spark_table()


if __name__ == '__main__':
    pass
    # main()
