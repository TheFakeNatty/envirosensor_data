import json
import os
import time
from pathlib import Path

current = os.path.dirname(os.path.realpath(__file__))
 
parent = os.path.dirname(current)
 
data_folder = os.path.join(current, 'Data')

def print_file(file_path):
    f = open(file_path)
    data = json.load(f)

    for entries in data: 
        print(entries)

def main():
    for folder in os.listdir(data_folder):
        if folder == 'README.md':
            pass
        else: 
            for file in os.listdir(str(data_folder) + "\\" + folder):
                print_file(str(data_folder) + "\\" + folder + "\\" + file)

if __name__ == "__main__":
    main()