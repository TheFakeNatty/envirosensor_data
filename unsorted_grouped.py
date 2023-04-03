import json
import os
import pymongo
from pymongo import MongoClient

db_url= "mongodb://localhost:27017/"
myclient = MongoClient(db_url)
db = myclient["Unsorted_Grouped"]

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
data_folder = os.path.join(current, 'data')


def main():
    print(data_folder)
    for folder in os.listdir(data_folder):
        if folder == 'README.md':
            pass
        else:
            for file in os.listdir(str(data_folder) + "\\" + folder):
                f = open(str(data_folder) + "\\" + folder + "\\" + file)
                data = json.load(f)

                collection_name = str(file.split('.')[0])
                collection = db[collection_name]

                for value in data:
                    collection.insert_one(value)

if __name__ in "__main__":
    main()