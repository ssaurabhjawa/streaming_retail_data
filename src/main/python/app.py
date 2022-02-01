import os
from util import get_spark_session
from transform import read_data_from_topic
from pyspark.sql import functions as F

appName = os.environ.get('NAME')
folder_path = os.environ.get('PATH')
env = os.environ.get('ENV')


def main():
    spark = get_spark_session(env, appName)
    read_data = spark.read.json(folder_path)
    primary_data = read_data.select('*', F.input_file_name().alias('file_path'))
    for table in primary_data.select('table_name').distinct().toLocalIterator():
        datasets = read_data_from_topic(primary_data, f'{table.table_name}')
        print(datasets)


if __name__ == '__main__':
    main()
