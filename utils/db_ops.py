import json
import os
from pathlib import Path
from pymongo import MongoClient

def unsorted_push(url, dbname, db_collection, data_folder):
    db_client = MongoClient(url)
    db = db_client[dbname]
    collection = db[db_collection]

    print(f"Pushing data to {dbname}, collection {db_collection}.....")

    for folder in os.listdir(data_folder):
        if folder == 'README.md':
            pass
        else:
            for file in os.listdir(Path( data_folder / folder)):
                f = open(Path(data_folder / folder / file))
                data = json.load(f)

                for value in data:
                    collection.insert_one(value)

if __name__ == "__main__":
    pass
