import json
import os
import time
from pathlib import Path
from utils import handler

current = os.path.dirname(os.path.realpath(__file__))
 
parent = os.path.dirname(current)
 
data_folder = os.path.join(current, 'Data\\ICRI_Envirosensor_Data')

def makedir(path):
    if os.path.exists(path):
        pass
    else:
        os.mkdir(path)

def make_file(value, week):
    folder = str(current) + "\\data\\new_data\\wk" + str(week)
    data= json.loads(value)
    device_id = data['DeviceID']
    device_time = data['Time']
    device_data = data['Data']
    payload = {}
    payload[device_time] = device_id
    payload.update(device_data)
    device_file = Path(str(folder) + "\\" + str(device_id) + ".json")
    Path(device_file).touch()
    json_object = json.dumps(payload, indent=4, separators=(',',': '))
    with open(device_file, 'a') as f:
        f.write(json_object)
        f.write(",\n")

    with open(device_file, 'rb+') as fh:
        fh.seek(-1,2)
        fh.truncate()        

def main():

    start = time.time()
    print ()
    print(str(current) + "\\data\\new_data")
    week = 1
    for file in os.listdir(data_folder):
        makedir(str(current) + "\\data\\new_data\\wk" + str(week))

        f = open(str(data_folder) + "\\" + str(file))
        data = json.load(f)

        for entries in data:
            for key, value in entries.items():
                if key == 'data':
                    make_file(value, week)                    

        f.close()
        week = week + 1

        delta = time.time()

        delta_time = delta - start

        print("Time to compile week " + str(week) + " is: " + str(delta_time))

    end = time.time()
    total_time = end - start

    print("Total time for completion: " + str(total_time))

if __name__ == "__main__":
    main()