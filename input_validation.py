import json
import os
import time
from pathlib import Path

currentpath = Path(__file__)
rootpath = Path(currentpath).parent
data_folder = Path(rootpath / "data")

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
            for file in os.listdir(Path(data_folder / folder)):
                print_file(Path(data_folder / folder / file))

if __name__ == "__main__":
    main()