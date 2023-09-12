#!/bin/bash -xe
upload_log() {
  aws s3 cp /tmp/userdata_execution.log s3://demoytuserdataairflow/logs/
}

trap 'upload_log' EXIT

sudo -u ubuntu -i <<'EOF'

exec &>> /tmp/userdata_execution.log


sudo apt update
sudo apt -y install awscli
sudo apt --yes install python3-pip
sudo apt --yes install sqlite3
sudo apt-get --yes install libpq-dev
pip3 install --upgrade awscli
sudo pip3  install virtualenv 
python3 -m virtualenv  /home/ubuntu/venv 
source /home/ubuntu/venv/bin/activate
pip install "apache-airflow[postgres]==2.5.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.5.0/constraints-3.7.txt" pandas boto3 
airflow db init
sudo apt-get --yes install postgresql postgresql-contrib
sudo -i -u postgres <<'EOpostgres'
psql -U postgres -c "CREATE DATABASE airflow;"
psql -U postgres -c "CREATE USER airflow WITH PASSWORD 'airflow';"
psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;"
EOpostgres
sed -i 's#sqlite:////home/ubuntu/airflow/airflow.db#postgresql+psycopg2://airflow:airflow@localhost/airflow#g' /home/ubuntu/airflow/airflow.cfg
sed -i 's#SequentialExecutor#LocalExecutor#g' /home/ubuntu/airflow/airflow.cfg
airflow db init
airflow users create -u airflow -f airflow -l airflow -r Admin -e airflow@gmail.com -p admin@123!
mkdir /home/ubuntu/dags
aws s3 cp s3://demoytuserdataairflow/dags/hello_world.py /home/ubuntu/dags
sed -i 's/^dags_folder = .*/dags_folder = \/home\/ubuntu\/dags/' /home/ubuntu/airflow/airflow.cfg
sed -i 's/^load_examples = .*/load_examples = False/' /home/ubuntu/airflow/airflow.cfg
airflow db init
EOF