### Installation

Create a new virtuanenv and activate it
```sh
$ virtualenv env_name
$ source ./env_name/bin/activate
$ cd ingestor
```
Install the requirements in your new virtual environment
```sh
$ pip3 install -r requirements.txt
```
or
```sh
$ pip install -r requirements.txt
```
Set path to the airflow_home
```sh
$ export AIRFLOW_HOME=$(pwd)
$ export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
```
initialize airflow db
and start webserver and scheduler
```sh
$ airflow initdb
$ airflow webserver --port 8080
$ airflow scheduler
```
## Upload variables in Airflow UI
Menu -> Admin > Variables > Choose file
And airflow_home/select variables.json

Open the following url in the browser to see Airflow UI
```sh
localhost:8080
```