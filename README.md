# Datalake for Cities in the US
#### The purpose of this project is to demonstrate how AWS S3, Apache Airflow, Dask, and AWS Redshift in order to show how to create a complete data pipeline. As well as have the data be utilized by a person to learn using an interactive graph for each city in the database. 
## Dependencies
Install the necessary python libraries listed below:
- numpy
- pandas
- bokeh
- dask
- apache-dataflow
- statsmodels
- scikit-learn
- facebook-prophet

## Data Resources:
1) NCHS reports on births and fertilities by cities in US. Source prodived by NCHS on [Data.gov](https://data.cdc.gov/NCHS/NCHS-Births-and-General-Fertility-Rates-United-Sta/e6fc-ccez).
2) US Government reports on US cities and diseases. Source from [Data.gov](https://catalog.data.gov/dataset/u-s-chronic-disease-indicators-cdi).
3) Climate and Other Weather data about Cities. Source provided by [Kaggle](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data/download).
4) US Cities Cost of Living and Demographics data. Source is found [here](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/).
5) Crime rates prodived by the FBI. [FBI.crimereport.gov](https:www.google.com).
6) Schooling ratings for cities in the US. Source found on [Kaggle](https:www.google.com).
## AWS SetUp and Access
Set up two S3 bucket to store all the data and another to place the wrangled data.

- The data wrangling uses Apache Spark to do ETL on the data.
- You will need to set up the AWS credentials

## Star Schema of the Database
- Insert basic image of the database here.

## Interactive graph for users 
Based on the selected data fields the user can interact with the graph and see what the data is output and the user can make their own conclusion about the city.

