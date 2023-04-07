import json
import os
import time
from pathlib import Path

currentpath = Path(__file__)
rootpath = Path(currentpath).parent
data_folder = Path(rootpath / "Data" / "ICRI_Envirosensor_Data")


def makedir(path):
    if os.path.exists(path):
        pass
    else:
        os.mkdir(path)

def make_file(value, week):
    wk = "wk" + str(week)
    folder = Path ( rootpath / "data" / "new_data" / wk)
    data= json.loads(value)
    device_id = data['DeviceID']
    device_time = data['Time']
    device_data = data['Data']
    payload = {}
    payload["ID"] = device_id
    payload["TIME"] = device_time
    payload.update(device_data)
    file_name = str(device_id) + ".json"
    device_file = Path(folder / file_name)
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

    week = 1
    for file in os.listdir(data_folder):
        wk = "wk" + str(week)
        makedir(Path(rootpath / "data" / "new_data" / wk))

        f = open(Path(data_folder) / str(file))
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