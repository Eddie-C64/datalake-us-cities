from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
import configparser
from datetime import datetime
import boto3
import os

config = configparser.ConfigParser()
config.read('dl.cfg')

KEY = configparser.ConfigParser.get("AWS", "KEY")
SECRET = configparser.ConfigParser.get("AWS", "SECRET")


def create_spark_session():
    spark = SparkSession.builder.\
        config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5"). \
        getOrCreate()
    return spark


def get_csv_from_s3(s3_file_path):
    s3 = boto3.resource('s3',
                        region_name='us-west-2',
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET)
    us_cities_bucket = s3.Bucket('uscitiesdataraw')

    for csv_file in us_cities_bucket.objects():
        print("======= LOADING CSV: ** {} ** =======".format(csv_file))


def process_data_temp(spark, input_data, output_data):
    temp = input_data + 'us_city_temp_date.csv'
    temp_df = spark.read.csv(temp)
    temp_table = (
        temp_df.select(
            col('dt').alias('date_time'),
            col('AverageTemperature').alias('avg_temp'),
            col('AverageTemperatureUncertainty').alais('avg_temp_uncertainty'),
            col('City').alias('city_name'),
            col('Latitude').alias('Latitude'),
            col('Longitude').alias('longitude')))
    temp_table.write.parquert(output_data+'births.parquet', mode='overwrite')
    print("Temperature Table has been written into S3")

    disease = input_data + 'chronic_disease_Indicators.csv'
    disease_df = spark.read.csv(disease)
    disease_table = (
        disease_df.select(
                col('YearStart').alias('year'),
                col('LocationAbbr').alias('Location_abbr'),
                col('Topic').alais('topic'),
                col('DataValueType').alias('value_type'),
                col('DataValue').alias('Latitude'),
                col('DataValueAlt').alias('value_alt')))
    disease_table.write.parquert(output_data + 'disease.parquet', mode='overwrite')
    print("Disease Table has been written into S3")

    demo = input_data + 'us_cities_demographics.csv'
    demo_df = spark.read.csv(demo)
    demo_table = (
        disease_df.select(
            col('City').alias('city_name'),
            col('State').alias('state'),
            col('Topic').alais('topic'),
            col('Median Age').alias('median_age'),
            col('Male Population').alias('male_pop'),
            col('Female Population').alias('female_pop'),
            col('Total Population').alias('total_pop'),
            col('Foreign-born').alias('foreign_born'),
            col('Average Household Size').alias('avg_household_size'),
            col('State Code').alias('state_abb'),
            col('Race').alias('most_common_race'),
            col('Count').alias('most_common_race_pop')
        ))
    demo_table.write.parquert(output_data + 'demo.parquet', mode='overwrite')
    print("Demographics Table has been written into S3")

    cost = input_data + 'cost_of_living_index.csv'
    births = input_data + 'births_and_general_gertility_rates.csv'


def main():
    csv_list = []
    spark = create_spark_session()

    input_data = 's3://uscitiesdataraw/'
    output_data = 's3://uscitiesdatalake/'


if __name__ == '__main__':
    main()

