import os
from read import read_send_message
from consumer import spark_consumer, kafka_consumer

def main():
    env = os.environ.get('ENVIRON')
    src_dir=os.environ.get('SRC_DIR')
    src_file_format=os.environ.get('SRC_FILE_FORMAT')
    tgt_dir=os.environ.get('TGT_DIR')
    tgt_file_format=os.environ.get('TGT_FILE_FORMAT')
    read_send_message(src_dir)
    spark_consumer(env, 'retail_k_consumer')
    kafka_consumer(env, 'retail_k_consumer')

if __name__ == '__main__':
    main()