import datetime
import os
from pathlib import Path

import utils.db_ops as db_ops

db_url= "mongodb://localhost:27017/"
db = "Test-Data"
collection = 'IoT_Devices'

def main():
    currentpath = Path(__file__)
    rootpath = Path(currentpath).parent
    data_folder = Path(rootpath / "data")

    start_time = datetime.datetime.now()
    db_ops.unsorted_grouped_push(db_url, db, collection, data_folder)
    end_time = datetime.datetime.now()

    total_time = end_time - start_time
    print (f"Elapsed time: {total_time}")
if __name__ == "__main__":
    main()