import json
import os
import pymongo
from bson.timestamp import Timestamp
import datetime
from pathlib import Path
from pymongo import MongoClient

ts = datetime.datetime.now()

db_url= "mongodb://localhost:27017/"
myclient = MongoClient(db_url)
db = myclient["Sorted_Grouped"]

currentpath = Path(__file__)
rootpath = Path(currentpath).parent
data_folder = Path(rootpath / "data")


def main():
    for folder in os.listdir(data_folder):
        if folder == "README.md":
            pass
        else:
            for file in os.listdir(Path(data_folder / folder)):
                try:
                    collection_name = str(file.split('.')[0])
                    db.create_collection(collection_name, timeseries={ 'timeField': 'TIME' })
                except:
                    print("Collection Already Exists...")

    for folder in os.listdir(data_folder):
        if folder == 'README.md':
            pass
        else:
            for file in os.listdir(Path( data_folder / folder)):
                f = open(Path(data_folder / folder / file))
                data = json.load(f)

                collection_name = str(file.split('.')[0])
                collection = db[collection_name]
                
                for value in data:
                    try:
                        time_value = value["TIME"]
                        # Convert string to datetime object
                        dt = datetime.datetime.strptime(time_value, "%Y-%m-%d %H:%M:%S.%f")

                        # Round microseconds to three decimal places
                        rounded_microseconds = round(dt.microsecond, -3)

                        # Create a new datetime object with rounded microseconds
                        rounded_dt = dt.replace(microsecond=rounded_microseconds)

                        # Format datetime object to desired format
                        formatted_time = rounded_dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4]

                        formatted_time = formatted_time + "+00:00"

                        formatted_time = datetime.datetime.strptime(formatted_time, "%Y-%m-%dT%H:%M:%S.%f%z")
                    except:
                        formatted_time = datetime.datetime.now()

                    try:
                        value["ID"] = value["ID"]
                    except:
                        value["ID"] = ""
                    try:
                        value["TIME"] = value["TIME"]
                    except:
                        value["TIME"] = ""
                    try:
                        value["TMP"] = value["TMP"]
                    except:
                        value["TMP"] = ""
                    try:
                        value["OPT"] = value["OPT"]
                    except:
                        value["OPT"] = ""
                    try:
                        value["BAT"] = value["BAT"]
                    except:
                        value["BAT"] = ""
                    try:
                        value["HDT"] = value["HDT"]
                    except:
                        value["HDT"] = ""
                    try:
                        value["BAR"] = value["BAR"]
                    except:
                        value["BAR"] = ""
                    try:
                        value["HDH"] = value["HDH"]
                    except:
                        value["HDH"] = ""

                    collection.insert_one(
                        {
                            "metadata": { "ID": value["ID"], "type": "EnviroSensor" },
                            "TIME": formatted_time,
                            "OPT": value['OPT'],
                            "TMP": value['TMP'],
                            "BAT": value['BAT'],
                            "HDT": value['HDT'],
                            "BAR": value['BAR'],
                            "HDH": value['HDH']
                        
                        }
                    )

if __name__ in "__main__":
    main()