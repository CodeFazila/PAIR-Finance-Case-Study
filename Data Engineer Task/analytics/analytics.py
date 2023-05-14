from os import environ
from time import time, sleep, strftime
from sqlalchemy import (Column, Integer, Float, String, create_engine,
                        insert, MetaData, select, and_, text, Table, TIMESTAMP)
from sqlalchemy.exc import OperationalError
from geopy.distance import distance as geopy_distance
from datetime import datetime, timezone
import json

print('Waiting for the data generator...')
sleep(60)
print('ETL Starting...')

while True:
    try:
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
        mysql_engine = create_engine(environ["MYSQL_CS"], pool_pre_ping=True, pool_size=10)
        psql_connection = psql_engine.connect()
        mysql_connection = mysql_engine.connect()
        
        # Define the table schema
        psql_metadata = MetaData()
        mysql_metadata = MetaData()

        aggregated_data = Table('aggregated_data', mysql_metadata,
                       Column('id', Integer, primary_key=True),
                       Column('device_id', String(length=50)),
                       Column('date', TIMESTAMP),
                       Column('max_temperature', Integer),
                       Column('data_points', Integer),
                       Column('distance', Float))
        
        # Drop the table if it exists
        aggregated_data.drop(mysql_engine, checkfirst=True)        
        mysql_metadata.create_all(mysql_engine)
        break
    except OperationalError:
        sleep(0.1)
        
print('Connection to PostgresSQL successful.')
print('Connection to MySQL successful.')

#variables definition
last_processed_time = 0

#function to return current hour from epoch in UTC timezone
def get_last_hour_from_epoch(epoch_time):
    time_obj = datetime.utcfromtimestamp(epoch_time)
    hour_str = time_obj.strftime('%Y-%m-%d %H:00:00')
    hour = datetime.strptime(hour_str, '%Y-%m-%d %H:%M:%S')

    return hour

#function that computes data aggregations and returns a tuple consisting of dictionaries and the last processed time. 
#The last processed time var is being used to filter records that have been fetched before.
def aggregate_data(results, last_processed_time):
    
    # Initialize dictionaries to store the aggregations for each device per hours
    max_temperatures = {}
    data_points = {}
    distances = {}
    prev_location = None
    
    # Iterate over each data point and calculate the aggregations
    for row in results:
        device_id, temperature, location = row.device_id, row.temperature, row.location
        hour_str = get_last_hour_from_epoch(int(row.time)) 
        
        # Calculate the maximum temperature for each device per hour
        if device_id not in max_temperatures:
            max_temperatures[device_id] = {}
        if hour_str not in max_temperatures[device_id]:
            max_temperatures[device_id][hour_str] = temperature
        else:
            max_temperatures[device_id][hour_str] = max(max_temperatures[device_id][hour_str], temperature)
        
        # Calculate the amount of data points aggregated for each device per hour
        if device_id not in data_points:
            data_points[device_id] = {}
        if hour_str not in data_points[device_id]:
            data_points[device_id][hour_str] = 1
        else:
            data_points[device_id][hour_str] += 1
        
        # Calculate the total distance of device movement for each device per hour
        if device_id not in distances:
            distances[device_id] = {}
        if hour_str not in distances[device_id]:
            distances[device_id][hour_str] = 0
        # Calculate the distance between the current and previous location
        
        current_location_str = list(json.loads(location).values())
        current_location = (current_location_str[0], current_location_str[1])
        if prev_location:
            distance_km  = geopy_distance(prev_location, current_location).km
            distances[device_id][hour_str] += distance_km 
        prev_location = current_location
        
        # Update last processed time
        last_processed_time = int(row.time)
        
    return max_temperatures, data_points, distances, last_processed_time
        
#function to insert the aggregated data into the provided MySQL database
def insert_aggregated_data(max_temperatures, data_points, distances):
    for device_id, hours in max_temperatures.items():
        for hour, max_temperature in hours.items():
            mysql_connection.execute(insert(aggregated_data).values(
                device_id=device_id,
                date=hour,
                max_temperature=max_temperature,
                data_points=data_points[device_id][hour],
                distance=distances[device_id][hour]
            ))
    mysql_connection.commit()      

while True:

    # Pull the data from PostgresSQL
    psql_metadata.reflect(bind=psql_engine, only=['devices'])
    devices = psql_metadata.tables['devices']
    
    # Get the epoch time of the last hour
    current_time = int(time())  
    current_hour_start = current_time - (current_time % 3600)  

    # Build the SELECT statement to fetch data from devices table
    select_stmt = select(devices).where(
        and_(
            text(f"CAST(devices.time AS INTEGER) > {last_processed_time}"),
            text(f"CAST(devices.time AS INTEGER) < {current_hour_start}")
        )
    ).order_by(devices.c.time.asc())
    results = psql_connection.execute(select_stmt)

    max_temperatures, data_points, distances, last_processed_time = aggregate_data(results, last_processed_time)

    insert_aggregated_data(max_temperatures, data_points, distances)


    # It will sleep until the current hour finishes and then it will dump the aggreagted data into MySQL db.
    # There are multiple ways to handle data of current running hour, some of them I have mentioned in Git Readme file. 
    current_time_utc = datetime.now(timezone.utc)
    next_hour = current_time_utc.replace(hour=current_time_utc.hour+1, minute=0, second=0, microsecond=0)
    sleep_time = (next_hour - current_time_utc).seconds
    sleep(sleep_time)


