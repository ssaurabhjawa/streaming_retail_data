from confluent_kafka import Consumer
from util import get_spark_session
from pyspark.sql.functions import date_format, to_date, split, substring
import getpass
def kafka_consumer(env, appName):
    c = Consumer({
        'bootstrap.servers': 'cdp03.itversity.com:9092,cdp04.itversity.com:9092,cdp05.itversity.com:9092',
        'group.id': 'mygroup',
        'auto.offset.reset': 'earliest'
    })
    c.subscribe(['retail_topic_1'])
    while True:
        msg = c.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        print('Received message: {}'.format(msg.value().decode('utf-8')))
    c.close()

def spark_consumer(env, appName):
    username = getpass.getuser()
    spark = get_spark_session(env, appName)
    kafka_bootstrap_servers = 'cdp03.itversity.com:9092,cdp04.itversity.com:9092,cdp05.itversity.com:9092'
    df = spark. \
        readStream. \
        format('kafka'). \
        option('kafka.bootstrap.servers', kafka_bootstrap_servers). \
        option('subscribe', 'retail_topic_1'). \
        option("startingOffsets", "earliest"). \
        load()


    df.selectExpr("CAST(value AS STRING)"). \
        withColumn('log_date', to_date(substring(split('value', ' ')[3], 2, 21), '[dd/MMM/yyyy:HH:mm:ss')). \
        withColumn('year', date_format('log_date', 'yyyy')). \
        withColumn('month', date_format('log_date', 'MM')). \
        withColumn('dayofmonth', date_format('log_date', 'dd')). \
        writeStream. \
        partitionBy('year', 'month', 'dayofmonth'). \
        format('csv'). \
        option("checkpointLocation", f'/user/{username}/kafka/retail_logs/gen_logs/checkpoint'). \
        option('path', f'/user/{username}/kafka/retail_logs/gen_logs/data'). \
        trigger(processingTime='30 seconds'). \
        start()

    df.selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value").printSchema()
    df.printSchema()
    df.show(truncate=False)