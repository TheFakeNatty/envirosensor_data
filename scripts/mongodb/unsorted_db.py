import json
import os
import pymongo
import datetime
from pathlib import Path
from pymongo import MongoClient

db_url= "mongodb://localhost:27017/"
myclient = MongoClient(db_url)
db = myclient["Test-Data"]
collection = db['IoT_Devices']

currentpath = Path(__file__)
rootpath = Path(currentpath).parent
data_folder = Path(rootpath / "data")


def main():

    start_time = datetime.datetime.now()

    print(data_folder)
    for folder in os.listdir(data_folder):
        if folder == 'README.md':
            pass
        else:
            for file in os.listdir(Path( data_folder / folder)):
                f = open(Path(data_folder / folder / file))
                data = json.load(f)

                for value in data:
                    collection.insert_one(value)
    
    end_time = datetime.datetime.now()

    total_time = end_time - start_time
    print (f"Elapsed time: {total_time}")

if __name__ in "__main__":
    main()