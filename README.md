# BrightData
BrightData Assignment

# NYC Taxi Trip Data

This repository contains Python script that can be used to download and load NYC taxi trip data into a MySQL database.

## Prerequisites

* Python 3.10.5 or higher
* MySQL 5.7 or higher
* The `requests`,`pandas`,`os`,`sqlalchemy`,`dotenv` Python libraries

## How to use

1. Clone the repository to your local machine.
2. Install the relevant Python libraries.
3. Open a terminal window and navigate to the directory where you cloned the repository.
4. Add the relevant MySQL connection details to the .env file
5. Run the following command:

python bright_data.py


the output will be 2 MySQL tables:

1. yellow_taxi_trips - raw data table
2. yellow_taxi_trips_agg - aggregation data table



