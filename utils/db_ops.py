import json
import os
from bson.timestamp import Timestamp
import datetime
from pathlib import Path
from pymongo import MongoClient

def unsorted_push(url, dbname, db_collection, data_folder):
    db_client = MongoClient(url)
    db = db_client[dbname]
    collection = db[db_collection]

    print(f"Pushing Unsorted data to {dbname}, collection {db_collection}.....")

    for folder in os.listdir(data_folder):
        if folder == 'README.md':
            pass
        else:
            for file in os.listdir(Path( data_folder / folder)):
                f = open(Path(data_folder / folder / file))
                data = json.load(f)

                for value in data:
                    collection.insert_one(value)


def unsorted_grouped_push(url, dbname, db_collection, data_folder):
    db_client = MongoClient(url)
    db = db_client[dbname]
    collection = db[db_collection]

    print(f"Pushing Unsorted_Grouped data to {dbname}, collection {db_collection}.....")

    for folder in os.listdir(data_folder):
        if folder == 'README.md':
            pass
        else:
            for file in os.listdir(Path(data_folder / folder)):
                f = open(Path(data_folder / folder / file))
                data = json.load(f)

                collection_name = str(file.split('.')[0])
                collection = db[collection_name]

                for value in data:
                    collection.insert_one(value)

def sorted_unoptimized_push(url, dbname, db_collection, data_folder):
    db_client = MongoClient(url)
    db = db_client[dbname]
    collection = db[db_collection]

    try:
        db.create_collection(db_collection, timeseries={ 'timeField': 'TIME', 'metaField': 'metadata' })
    except:
        print("Either the collection already exists of something failed...")

    print(f"Pushing Sorted data to {dbname}, collection {db_collection}.....")

    for folder in os.listdir(data_folder):
        if folder == 'README.md':
            pass
        else:
            for file in os.listdir(Path( data_folder / folder)):
                f = open(Path(data_folder / folder / file))
                data = json.load(f)
                
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
                        value["TMP"] = float(value["TMP"])
                    except:
                        value["TMP"] = ""
                    try:
                        value["OPT"] = float(value["OPT"])
                    except:
                        value["OPT"] = ""
                    try:
                        value["BAT"] = float(value["BAT"])
                    except:
                        value["BAT"] = ""
                    try:
                        value["HDT"] = float(value["HDT"])
                    except:
                        value["HDT"] = ""
                    try:
                        value["BAR"] = float(value["BAR"])
                    except:
                        value["BAR"] = ""
                    try:
                        value["HDH"] = float(value["HDH"])
                    except:
                        value["HDH"] = ""

                    for k, v in value.items():
                        if v != "":
                            try:
                                if k == "TMP":
                                    collection.insert_one({
                                        "metadata": {
                                            "ID": value["ID"], 
                                            "type": "Ambient Temperature",
                                        },
                                        "TIME": formatted_time,
                                        "TMP": float(v)
                                    })
                                elif k == "OPT":
                                    collection.insert_one({
                                        "metadata": {
                                            "ID": value["ID"], 
                                            "type": "Ambient Light",
                                        },
                                        "TIME": formatted_time,
                                        "OPT": float(v)
                                    })
                                elif k == "BAT":
                                    collection.insert_one({
                                        "metadata": {
                                            "ID": value["ID"], 
                                            "type": "Temperature",
                                        },
                                        "TIME": formatted_time,
                                        "BAT": float(v)
                                    })
                                elif k == "HDT":
                                    collection.insert_one({
                                        "metadata": {
                                            "ID": value["ID"], 
                                            "type": "Ambient Temperature",
                                        },
                                        "TIME": formatted_time,
                                        "HDT": float(v)
                                    })
                                elif k == "BAR":
                                    collection.insert_one({
                                        "metadata": {
                                            "ID": value["ID"], 
                                            "type": "Barometric Pressure",
                                        },
                                        "TIME": formatted_time,
                                        "BAR": float(v)
                                    })
                                elif k == "HDH":
                                    collection.insert_one({
                                        "metadata": {
                                            "ID": value["ID"], 
                                            "type": "Humidity",
                                        },
                                        "TIME": formatted_time,
                                        "HDH": float(v)
                                    })
                            except:
                                print("Failed to push value" + v)

def sorted_unoptimized_grouped_push(url, dbname, db_collection, data_folder):
    db_client = MongoClient(url)
    db = db_client[dbname]
    collection = db[db_collection]
    ts = datetime.datetime.now()
    for folder in os.listdir(data_folder):
        if folder == "README.md":
            pass
        else:
            for file in os.listdir(Path(data_folder / folder)):
                try:
                    collection_name = str(file.split('.')[0])
                    db.create_collection(collection_name, timeseries={ 'timeField': 'TIME', 'metaField': 'metadata' })
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
                print(collection_name)
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
                        value["TMP"] = float(value["TMP"])
                    except:
                        value["TMP"] = ""
                    try:
                        value["OPT"] = float(value["OPT"])
                    except:
                        value["OPT"] = ""
                    try:
                        value["BAT"] = float(value["BAT"])
                    except:
                        value["BAT"] = ""
                    try:
                        value["HDT"] = float(value["HDT"])
                    except:
                        value["HDT"] = ""
                    try:
                        value["BAR"] = float(value["BAR"])
                    except:
                        value["BAR"] = ""
                    try:
                        value["HDH"] = float(value["HDH"])
                    except:
                        value["HDH"] = ""

                    for k, v in value.items():
                        if v != "":
                            try:
                                if k == "TMP":
                                    collection.insert_one({
                                        "metadata": {
                                            "ID": value["ID"], 
                                            "type": "Ambient Temperature",
                                        },
                                        "TIME": formatted_time,
                                        "TMP": float(v)
                                    })
                                elif k == "OPT":
                                    collection.insert_one({
                                        "metadata": {
                                            "ID": value["ID"], 
                                            "type": "Ambient Light",
                                        },
                                        "TIME": formatted_time,
                                        "OPT": float(v)
                                    })
                                elif k == "BAT":
                                    collection.insert_one({
                                        "metadata": {
                                            "ID": value["ID"], 
                                            "type": "Temperature",
                                        },
                                        "TIME": formatted_time,
                                        "BAT": float(v)
                                    })
                                elif k == "HDT":
                                    collection.insert_one({
                                        "metadata": {
                                            "ID": value["ID"], 
                                            "type": "Ambient Temperature",
                                        },
                                        "TIME": formatted_time,
                                        "HDT": float(v)
                                    })
                                elif k == "BAR":
                                    collection.insert_one({
                                        "metadata": {
                                            "ID": value["ID"], 
                                            "type": "Barometric Pressure",
                                        },
                                        "TIME": formatted_time,
                                        "BAR": float(v)
                                    })
                                elif k == "HDH":
                                    collection.insert_one({
                                        "metadata": {
                                            "ID": value["ID"], 
                                            "type": "Humidity",
                                        },
                                        "TIME": formatted_time,
                                        "HDH": float(v)
                                    })
                            except:
                                print("Failed to push value" + v)

def sorted_optimized_grouped_push(url, dbname, db_collection, data_folder):
    db_client = MongoClient(url)
    db = db_client[dbname]
    collection = db[db_collection]
    ts = datetime.datetime.now()
    for folder in os.listdir(data_folder):
        if folder == "README.md":
            pass
        else:
            for file in os.listdir(Path(data_folder / folder)):
                try:
                    collection_name = str(file.split('.')[0])
                    db.create_collection(collection_name, timeseries={ 'timeField': 'TIME', 'metaField': 'metadata' })
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
                print(collection_name)
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

                    if value["TIME"] == None:
                        pass
                    else:
                        collection.insert_one({
                            "metadata": {
                                "ID": value["ID"], 
                                "type": "EnviroSensor",
                            },
                            "TIME": formatted_time,
                            "reading": {
                                key: value[key] for key in ["TMP", "OPT", "BAT", "HDT", "BAR", "HDH"] if value[key] is not None
                            }
                        })

def sorted_optimized_push(url, dbname, db_collection, data_folder):
    db_client = MongoClient(url)
    db = db_client[dbname]
    collection = db[db_collection]

    try:
        db.create_collection(db_collection, timeseries={ 'timeField': 'TIME', 'metaField': 'metadata' })
    except:
        print("Either the collection already exists of something failed...")

    print(f"Pushing Sorted data to {dbname}, collection {db_collection}.....")

    for folder in os.listdir(data_folder):
        if folder == 'README.md':
            pass
        else:
            for file in os.listdir(Path( data_folder / folder)):
                f = open(Path(data_folder / folder / file))
                data = json.load(f)
                
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

                    if value["TIME"] == None:
                        pass
                    else:
                        collection.insert_one({
                            "metadata": {
                                "ID": value["ID"], 
                                "type": "EnviroSensor",
                            },
                            "TIME": formatted_time,
                            "reading": {
                                key: value[key] for key in ["TMP", "OPT", "BAT", "HDT", "BAR", "HDH"] if value[key] is not None
                            }
                        })

if __name__ == "__main__":
    pass
