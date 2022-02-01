import os
from util import get_spark_session

appName = os.environ.get('NAME')
env = os.environ.get('ENV')


def read_data_from_topic(data, table_name):
    spark = get_spark_session(env, appName)
    records = data. \
        filter(f'table_name = "{table_name}"'). \
        select('record')
    dataset_df = spark.read.json(records.select('record').rdd.map(lambda item: item.record))
    return dataset_df
