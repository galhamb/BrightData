# Imports

import pandas as pd
from sqlalchemy import create_engine
import requests
from dotenv import load_dotenv
import os


# Download desired files from web

files = ['https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet',
         'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-02.parquet']


# Load files to DF and add month code column in order to aggragte by it later

df_yellow_taxi_trips = pd.DataFrame()

try :

    for file in files:
        r = requests.get(file, allow_redirects=True)
        file_full_path = 'C:/ParquetFiles/'+file[48:]
        open(file_full_path, 'wb').write(r.content)
        df_yellow_taxi_trips_file = pd.read_parquet(file_full_path)
        year_key = file_full_path[32:36]
        month_key = file_full_path[37:39]
        df_yellow_taxi_trips_file['month_code'] = int(year_key + month_key)
        df_yellow_taxi_trips = pd.concat([df_yellow_taxi_trips_file, df_yellow_taxi_trips], ignore_index=True)

# Set connection to the DB

    load_dotenv()
    database = os.getenv('DATABASE')
    database_user = os.getenv('DATABASE_USER')
    database_password = os.getenv('DATABASE_PASSWORD')
        
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                       .format(user=database_user,
                               pw=database_password,
                               db=database))

# Add a correction to a column name mismatch exists in the original files

    df_yellow_taxi_trips['Airport_fee'] = df_yellow_taxi_trips['Airport_fee'].fillna(df_yellow_taxi_trips['airport_fee'])
    df_yellow_taxi_trips = df_yellow_taxi_trips.drop(columns=['airport_fee'])

# Raw data DB insert

    df_yellow_taxi_trips.to_sql(con=engine, name='yellow_taxi_trips', index=False, if_exists='replace')

# Create aggregation DF

    agg_df = df_yellow_taxi_trips.groupby('month_code')['passenger_count'].sum().astype(int).to_frame().reset_index()

# Aggregation data DB insert

    agg_df.to_sql(con=engine, name='yellow_taxi_trips_agg',index=False, if_exists='replace')

 
 
 # Handle the exception

except Exception as error:
   
    print("An exception occurred:", error)




