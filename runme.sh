export HADOOP_CONF_DIR="/etc/hadoop/conf"
export PYSPARK_PYTHON=python3
export ENVIRON=PROD
export SRC_DIR=/home/${USER}/streaming_retail_data/retail_topic_data
export TGT_FOLDER=''
export TGT_FILE_FORMAT=''
python3 app.py

