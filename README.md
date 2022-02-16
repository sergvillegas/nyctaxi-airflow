# nyctaxi-airflow

Data pipeline that imports the TLC data sets and transforms the format to Parquet

## Getting Started

1. To get a local copy up and running type `git clone https://github.com/sergvillegas/nyctaxi-airflow`

2. Go to the main directory `cd nyctaxi-airflow`

3. Create a new environment `python3 -m venv venv`

4. Activate the new environment `source venv/bin/activate`

5. run `pip install -r requirements.txt`

6. Set up AIRFLOW_HOME in your project directory `export AIRFLOW_HOME="$(pwd)"`

7. Initialise the Airflow database `airflow db init`

8. Start the Airflow scheduler `airflow scheduler -D`

9. Unpause the DAG `airflow dags unpause nyctaxi`

10. Trigger the DAG `airflow dags trigger nyctaxi`

## How It Works

On the first run, it will initialize with the default configurations which imports the TLC data sets 
for “Yellow” and “Green” taxis for the last year.

It will also create a config.csv file `projects/nyctaxi/config/config.csv` 
config.csv will keep a record of the files it can't find and which it will need to look for on the next run.

Finally, it removes the .gitkeep files 
in `projects/nyctaxi/tripdata/csv/renamed/.gitkeep`, `projects/nyctaxi/tripdata/pq/.gitkeep`
The purpose of these files was to let git create empty directories.

The out folder `projects/nyctaxi/out/` contains the results for this query:
- The average distance driven by yellow and green taxis per hour

The pq folder `projects/nyctaxi/tripdata/pq` contains the data in Parquet format

## Future Updates

- [ ] NYC weather data // This is pending
- [ ] Data cleaning // This is pending
