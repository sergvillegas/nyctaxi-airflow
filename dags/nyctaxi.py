import os
import csv
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import logging


from airflow.decorators import dag, task
@dag(schedule_interval=None, start_date=datetime(2022, 2, 15), catchup=False, tags=['nyctaxi'])
def nyctaxi():

    logging.basicConfig(level=logging.INFO)

    @task()
    def initialize():
        logging.info('initializing...')
        # on the first run, it checks for the existence of config.csv file with future trip data ranges
        if os.path.isfile('./projects/nyctaxi/config/config.csv'):
            # if it finds the file, it knows it is not the first run
            first_run = False
            with open('./projects/nyctaxi/config/config.csv', 'r') as configfile:
                # config.csv logs the taxis, years and months of the missing ranges
                reader = csv.reader(configfile)
                taxis = set()
                years = set()
                months = set()
                for row in reader:
                    taxis.add(row[0])
                    years.add(int(row[1]))
                    months.add(int((row[2])))
                taxis = list(taxis)
                years = list(years)
                months = list(months)
                # checks to see if config.csv is empty
                if years:
                    year_min = years[0]
                    year_max = years[-1] + 1
                    month_min = months[0]
                    month_max = months[-1] + 1
                else:
                    # if config.csv is empty, all downloads are complete
                    year_min = year_max = month_min = month_max = 0
                    logging.info('all data is downloaded')
        else:
            # if config.csv is not available, it initializes with the default configuration
            first_run = True
            taxis = ['yellow', 'green']
            year_min = 2021
            year_max = 2022
            month_min = 1
            month_max = 13

            # last, it gets rid of the .gitkeep files to prevent any collision
            folders = ['./projects/nyctaxi/tripdata/csv/renamed/', './projects/nyctaxi/tripdata/pq/']
            for folder in folders:
                with os.scandir(folder) as entries:
                    for entry in entries:
                        if entry.name == '.gitkeep':
                            os.remove(entry.path)

        return dict(taxis=taxis, year_min=year_min, year_max=year_max, month_min=month_min, month_max=month_max,
                    first_run=first_run)

    @task()
    def extract(init_dict: dict):
        if init_dict['taxis']:
            logging.info('getting csv...')
            csv_available = False
            # it sets csv_available to False, because at this point it doesn't know if it will find a file
            with open('./projects/nyctaxi/config/config.csv', 'w') as configfile:
                configfile_writer = csv.writer(configfile)
                for taxi in init_dict['taxis']:
                    for year in range(init_dict['year_min'], init_dict['year_max']):
                        for month in range(init_dict['month_min'], init_dict['month_max']):
                            month_str = str(month).zfill(2)
                            url = 'https://nyc-tlc.s3.amazonaws.com/trip+data/' + taxi + '_tripdata_' + str(
                                year) + '-' + month_str + '.csv'
                            r = requests.get(url, allow_redirects=True)
                            if r.status_code == requests.codes.ok:
                                # if it finds a file, it sets csv_available to true, i.e. data is available
                                csv_available = True
                                filename = url.split('/')[-1]
                                logging.info('processing ' + filename)
                                open('./projects/nyctaxi/tripdata/csv/' + filename, 'wb').write(r.content)

                                # it renames the pickup datetime column to normalize across
                                # yellow and green taxi tripdata
                                # to be able select and run stats on this column for both data sets
                                with open('./projects/nyctaxi/tripdata/csv/' + filename, 'r') as inFile, \
                                        open('./projects/nyctaxi/tripdata/csv/renamed/' + filename, 'w') as outfile:
                                    r = csv.reader(inFile)
                                    w = csv.writer(outfile)
                                    header = list()
                                    for i, row in enumerate(r):
                                        if i == 0:
                                            for col in row:
                                                if col == 'tpep_pickup_datetime' or col == 'lpep_pickup_datetime':
                                                    header.append('pickup_datetime')
                                                else:
                                                    header.append(col)
                                            w.writerow(header)
                                        else:
                                            w.writerow(row)

                                os.remove('./projects/nyctaxi/tripdata/csv/' + filename)
                            else:
                                # if it can't find a file, it logs the taxi, year and month
                                # to look for it on the next run
                                configfile_writer.writerow([taxi, year, month])
                                logging.info(
                                    'file not available: ' + taxi + '_tripdata_' + str(year) + '-' + str(month).zfill(
                                        2) + '.csv. I will look for it on the next run.')
            return csv_available

        else:
            return False

    @task()
    def transform(csv_available: bool):
        if csv_available:
            # transforms csv to parquet
            with os.scandir('./projects/nyctaxi/tripdata/csv/renamed/') as files:
                for file in files:
                    logging.info('loading to parquet ' + file.name)
                    df = pd.read_csv('./projects/nyctaxi/tripdata/csv/renamed/' + file.name, low_memory=False)
                    # converts the dataFrame to an apache arrow table
                    table = pa.Table.from_pandas(df, preserve_index=False)
                    # parquet with snappy, default, compression
                    pq.write_table(table, './projects/nyctaxi/tripdata/pq/' + file.name.replace('.csv', '') + '.parquet')
                    # removes the csv files to save disk space
                    os.remove(file.path)
                return True
        else:
            return False

    @task()
    def load(data: bool):
        if data:
            # The average distance driven by yellow and green taxis per hour
            logging.info('calculating trip distance')
            # reads all parquet files in tripdata
            table = pq.read_table('./projects/nyctaxi/tripdata/pq', columns=['pickup_datetime', 'trip_distance'])
            df = table.to_pandas()
            # creates a new column for the hour out of the pickup_datetime column
            df['hour'] = pd.to_datetime(df.pickup_datetime).dt.hour
            avg_distance_per_hour = df.groupby(['hour'])[['trip_distance']].mean()
            avg_distance_per_hour.to_csv('./projects/nyctaxi/out/trip_distance.csv')

    init = initialize()
    data_available = extract(init)
    transformed_data = transform(data_available)
    load(transformed_data)


nyctaxi_etl = nyctaxi()
