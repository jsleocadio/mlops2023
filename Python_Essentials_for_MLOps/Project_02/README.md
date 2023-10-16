# Podcast Data Pipeline using Airflow

This repository contains code for building a data pipeline to download and transcribe podcast episodes using Apache Airflow. The pipeline downloads episodes from a specified podcast feed, stores episode metadata in a SQLite database, downloads audio files, transcribes the audio to text, and logs the process.

## Pre-requisites

You can install the dependencies using this command:

```
pip install -r requirements.txt
```

Install Airflow

```
AIRFLOW_VERSION=2.7.1
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

Once installed, you can run it with:

```
airflow standalone
```

This will create the configuration folder at `~/airflow` and will run it at `localhost:8080`. The next step is to change the file at `~/airflow/airflow.cfg` with the path to your DAGs. In my case, it looked like this:

```
dags_folder = /home/jsleocadio/Projetos/Undergraduation/mlops2023/Python_Essentials_for_MLOps/Project_02/dags
load_examples = False
```

Install SQLite

```
wget https://www.sqlite.org/snapshot/sqlite-snapshot-202309111527.tar.gz
tar -xzvf sqlite-snapshot-202309111527.tar.gz
cd sqlite-snapshot-202309111527
./configure
make
sudo make install
sqlite3 --version
```

At the path your_path/mlops2023/Python_Essentials_for_MLOps/Project_02/dags, create a database in a new terminal session:

```
sqlite3 episodes.db
.databases
.quit
```

Create a connection with the database

```
airflow connections add 'podcasts' --conn-type 'sqlite' --conn-host 'your_path/mlops2023/Python_Essentials_for_MLOps/Project_02/dags/episodes.db'
```

Check if the connection was added correctly:

```
airflow connections get podcasts
```

## References

* [Dataquest's Portifolio](https://app.dataquest.io/m/999911)
* [Dataquest's GitHub Repository](https://github.com/dataquestio/project-walkthroughs/tree/master/podcast_summary)