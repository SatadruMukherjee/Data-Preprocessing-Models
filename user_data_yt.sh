#!/bin/bash -xe
exec &>> /tmp/userdata_execution.log

upload_log() {
  aws s3 cp /tmp/userdata_execution.log s3://demoytuserdata/logs/
  sudo shutdown now -h
}

trap 'upload_log' EXIT

sudo apt update
sudo apt -y install awscli
sudo apt -y install python3-pip
pip3  install --upgrade awscli
pip3  install boto3 pandas pyarrow fastparquet
aws s3 cp s3://demoytuserdata/script/news_fetcher.py .
python3 news_fetcher.py
aws s3 mv /home/ubuntu/news_data.parquet  s3://demoytuserdata/outputdirectory/