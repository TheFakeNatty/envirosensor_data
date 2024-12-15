import mysql.connector
import json

def push_json_to_mysql(json_file_path, db_config):
    # Establish a connection to the MySQL database
    conn = mysql.connector.connect(
        host=db_config['localhost'],
        user=db_config['root'],
        password=db_config['Rapter2012!'],
        database=db_config['database']
    )
    cursor = conn.cursor()

    # Read the JSON document
    with open(json_file_path, 'r') as file:
        data = json.load(file)

    # Insert the JSON data into the iot_devices table
    placeholders = ', '.join(['%s'] * len(data))
    columns = ', '.join(data.keys())
    sql = f"INSERT INTO iot_devices ({columns}) VALUES ({placeholders})"
    cursor.execute(sql, list(data.values()))

    # Commit the transaction
    conn.commit()

    # Close the cursor and connection
    cursor.close()
    conn.close()

# Example usage
db_config = {
    'host': 'localhost',
    'user': 'your_username',
    'password': 'your_password',
    'database': 'iot_devices'
}
json_file_path = 'path_to_your_json_file.json'
push_json_to_mysql(json_file_path, db_config)