import json
import os
from bson.timestamp import Timestamp
import datetime
from pathlib import Path
import mysql.connector

##### VAR Definitions #####

def create_db(db_name, cursor):
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")

    cursor.execute(f"USE {db_name}")

    # Create the 'devices' table
    devices_table = """
    CREATE TABLE IF NOT EXISTS devices (
        device_id INT AUTO_INCREMENT PRIMARY KEY,
        device_name VARCHAR(100) NOT NULL,
        location VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    cursor.execute(devices_table)

    start_date = "2018-01-01 19:18:46.671781"
    start_date_object = datetime.datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S.%f")
    end_date = "2018-12-30 19:18:46.671781"
    end_date_object = datetime.datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S.%f")

    start_year_week = start_date_object.strftime('%Y%U')
    end_year_week = end_date_object.strftime('%Y%U')
    next_start_year_week = int(start_year_week) + 1
    next_end_year_week = int(start_year_week) + 1

    # Create the 'sensor_readings' table
    sensor_readings_table = """
    CREATE TABLE IF NOT EXISTS sensor_readings (
        reading_id BIGINT AUTO_INCREMENT,
        device_id INT NOT NULL,
        timestamp DATETIME NOT NULL,
        TMP FLOAT,
        OPT FLOAT,
        BAT FLOAT,
        HDT FLOAT,
        BAR FLOAT,
        HDH FLOAT,
        PRIMARY KEY (reading_id, timestamp)
    )
    ENGINE=InnoDB 
    AUTO_INCREMENT=286802795 
    DEFAULT CHARSET=utf8 
    PARTITION BY RANGE (YEARWEEK(timestamp)) (
        PARTITION p201801 VALUES LESS THAN (201802),
        PARTITION p201802 VALUES LESS THAN (201803),
        PARTITION p201803 VALUES LESS THAN (201804),
        PARTITION p201804 VALUES LESS THAN (201805),
        PARTITION p201805 VALUES LESS THAN (201806),
        PARTITION p201806 VALUES LESS THAN (201807),
        PARTITION p201807 VALUES LESS THAN (201808),
        PARTITION p201808 VALUES LESS THAN (201809),
        PARTITION p201809 VALUES LESS THAN (201810),
        PARTITION p201810 VALUES LESS THAN (201811),
        PARTITION p201811 VALUES LESS THAN (201812),
        PARTITION p201812 VALUES LESS THAN (201901),
        PARTITION future VALUES LESS THAN MAXVALUE
    );
    """
    cursor.execute(sensor_readings_table)

    # Create the 'aggregated_data' table
    aggregated_data_table = """
    CREATE TABLE IF NOT EXISTS aggregated_data (
        agg_id BIGINT AUTO_INCREMENT PRIMARY KEY,
        device_id INT NOT NULL,
        period_start DATETIME NOT NULL,
        period_end DATETIME NOT NULL,
        avg_temperature FLOAT,
        max_temperature FLOAT,
        min_temperature FLOAT,
        avg_humidity FLOAT,
        max_humidity FLOAT,
        min_humidity FLOAT,
        avg_pressure FLOAT,
        max_pressure FLOAT,
        min_pressure FLOAT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (device_id) REFERENCES devices(device_id)
    )
    """
    cursor.execute(aggregated_data_table)

# Function for pushing data from the json
def push_to_db(db_name, cursor, data_folder):

    current_date = datetime.datetime.now()

    print(f"Pushing data from {data_folder} to MySQL database {db_name}.....")

    for folder in os.listdir(data_folder):
        if folder == 'README.md':
            continue
        else: 
            unique_device_id = set()
            for file in os.listdir(Path( data_folder / folder)):
                device_id = os.path.splitext(file)[0]
                
                query = "SELECT EXISTS(SELECT 1 FROM devices WHERE device_id = %s)"
                cursor.execute(query, (device_id,))
                exists = cursor.fetchone()[0]

                if exists:
                    print(f"Device with ID {device_id} already exists in the database")
                else:
                    insert_device_query = "INSERT INTO devices (device_id, device_name, location, created_at) VALUES (%s, %s, %s, %s)"
                    cursor.execute(insert_device_query, (device_id, device_id, 'Towson', current_date))
                    print(f"Device with ID {device_id} added to the database")

                f = open(Path(data_folder / folder / file))
                data = json.load(f)
                
                for value in data:
                    try:
                        value["ID"] = value["ID"]
                    except:
                        value["ID"] = None
                    try:
                        value["TIME"] = value["TIME"]
                    except:
                        value["TIME"] = None
                    try:
                        value["TMP"] = float(value["TMP"])
                    except:
                        value["TMP"] = None
                    try:
                        value["OPT"] = float(value["OPT"])
                    except:
                        value["OPT"] = None
                    try:
                        value["BAT"] = float(value["BAT"])
                    except:
                        value["BAT"] = None
                    try:
                        value["HDT"] = float(value["HDT"])
                    except:
                        value["HDT"] = None
                    try:
                        value["BAR"] = float(value["BAR"])
                    except:
                        value["BAR"] = None
                    try:
                        value["HDH"] = float(value["HDH"])
                    except:
                        value["HDH"] = None
                    insert_query = "INSERT INTO sensor_readings (device_id, timestamp, TMP, OPT, BAT, HDT, BAR, HDH) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                    try:
                        cursor.execute(insert_query, (device_id, value['TIME'], value['TMP'], value['OPT'], value['BAT'], value['HDT'], value['BAR'], value['HDH']))
                    except mysql.connector.Error as err:
                        print(f"Error: {err}")

                f.close()




if __name__ == "__main__":
    pass