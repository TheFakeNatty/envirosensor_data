import json
import os
from bson.timestamp import Timestamp
import datetime
from pathlib import Path
import mysql.connector

import utils.mysql_db_ops as mysql_db_ops

##### VAR Definitions #####
currentpath = Path(__file__)
rootpath = Path(currentpath).parent
data_folder = Path(rootpath / "data")
db_name = "envirosensor_data"

# Connect to the MySQL server
mydb = mysql.connector.connect(
    host="localhost",
    user="worker",
    password="Password123@",
)

cursor = mydb.cursor()

def main():
    start_time = datetime.datetime.now()

    mysql_db_ops.create_db(db_name, cursor)
    mysql_db_ops.push_to_db(db_name, cursor, data_folder)
    

    end_time = datetime.datetime.now()

    total_time = end_time - start_time
    print (f"Elapsed time: {total_time}")

if __name__ == "__main__":
    main()


# Commit and Close the Connection
mydb.commit()
mydb.close()