# envirosensor_data
Data from IEEE Data set and the python script to convert it to a single device file

### File Parser
The File Parser is intended to be run with the unzipped IEEE Dataset in the /data folder of the directory. 

IEEE Dataset: https://ieee-dataport.org/open-access/icri-envirosensor-data 

Documentation can be found in Confluence under Research Page

### Converter
The Converter script is written in powershell. The data that came out of the parser script was in a improper JSON format to be read. This converter scripts adds the formatting required to make these files understandable by the Python scripts.

### Input Validation
This is a python script that loops through the data folder to make sure all the conversions worked and the data is readable.

### DB Scripts 
These scripts are designed to push the data to the mongodb. There are 4 different categories for testing purposes on the optimization of the DB format. 