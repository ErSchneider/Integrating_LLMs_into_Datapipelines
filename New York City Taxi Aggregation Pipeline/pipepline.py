import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import sqlite3
import argparse
import pandas as pd

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


def split_dates(_, start_date, end_date):
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')

    current_date = start_date
    all_months = []

    while current_date <= end_date:
        all_months.append(current_date.strftime('%Y-%m'))
        current_date += relativedelta(months=1)

    return [(pd.read_parquet(f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{date}.parquet"), date) for date in all_months]

def aggregate_data(df_raw_date):
    df_raw, date = df_raw_date
    
    df_raw.columns = df_raw.columns.str.lower()

    df_raw['tpep_pickup_datetime'] = pd.to_datetime(df_raw['tpep_pickup_datetime'])
    df_raw['tpep_dropoff_datetime'] = pd.to_datetime(df_raw['tpep_dropoff_datetime'])
    df_raw = df_raw[['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance', 'tip_amount', 'total_amount', 'airport_fee']]
    df_raw = df_raw[(df_raw['passenger_count'] > 0) &
                        (df_raw['trip_distance'] > 0) &
                        (df_raw['tip_amount'] >= 0) &
                        (df_raw['total_amount'] > 0) &
                        (df_raw['airport_fee'] >= 0)]
    df_raw = df_raw[(df_raw['tpep_pickup_datetime'].dt.month == int(date[-1:])) & (df_raw['tpep_pickup_datetime'].dt.year == int(date[:-3]))]
    df_raw = df_raw[(df_raw['tpep_dropoff_datetime'] - df_raw['tpep_pickup_datetime']) < timedelta(hours=8)]

    #daylight saving time will produce empty hours

    df = df_raw.set_index('tpep_pickup_datetime')
    hourly_aggregated = df.resample('H').agg(['mean', 'count'])
    hourly_aggregated.columns = ['_'.join(col).strip() for col in hourly_aggregated.columns.values]


    result_columns = [col for col in hourly_aggregated.columns if 'mean' in col or 'passenger_count_count' in col]
    result_df = hourly_aggregated[result_columns]
    result_df = result_df.reset_index()
    result_df = result_df.rename({'passenger_count_count':'trip_count'}, axis=1)
    result_df =  result_df.dropna()
    result_df['tpep_pickup_datetime'] = result_df['tpep_pickup_datetime'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))

    result_df['tpep_dropoff_datetime_mean'] = result_df['tpep_dropoff_datetime_mean'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))

    return [row.to_dict() for _, row in result_df.iterrows()]

def write_to_db(entry):
    with sqlite3.connect('database.db') as connection:
        cursor = connection.cursor()

        data_values = [entry[x] for x in entry]
        cursor.execute('''
            INSERT INTO NYC_v0_output (tpep_pickup_datetime, tpep_dropoff_datetime_mean, passenger_count_mean, trip_count, trip_distance_mean, tip_amount_mean, total_amount_mean, airport_fee_mean)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', data_values)

        connection.commit()
        cursor.close()


class Options(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--start_date', type=str)
        parser.add_argument('--end_date', type=str)


def run_pipeline():

    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', type=str)
    parser.add_argument('--end_date', type=str)

    options = PipelineOptions()
    start_date = options.view_as(Options).start_date
    end_date = options.view_as(Options).end_date
    print(start_date, end_date)

    with beam.Pipeline() as pipeline:
        (
            pipeline
            | 'Erstellung' >> beam.Create([None])
            | 'Extraktion' >> beam.ParDo(split_dates, start_date=start_date, end_date=end_date)
            | 'Transformation' >> beam.FlatMap(aggregate_data)
            | 'Speicherung' >> beam.ParDo(write_to_db)
            #| 'PrintOutput' >> beam.Map(lambda x: print(x))
        )

if __name__ == '__main__':

    start = datetime.now()
    print(start)
    run_pipeline()
    end = datetime.now()
    print(end)
    print(end-start)