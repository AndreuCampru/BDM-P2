from hdfs import InsecureClient
import happybase
from avro.io import DatumReader
from avro.datafile import DataFileReader
from io import BytesIO
import json
from tqdm import tqdm
import base64
import sys

# Read data from Avro files in HDFS
def read_avro_file_from_hdfs(hdfs_client, hdfs_avro_path, filename):   
    # Read Avro file from HDFS
    with hdfs_client.read(f"{hdfs_avro_path}/{filename}") as reader: 
        # Wrap the HDFS reader in BytesIO
        hdfs_file_io = BytesIO(reader.read())
        
        # Create DataFileReader instance to read Avro data
        data = DataFileReader(hdfs_file_io, DatumReader())
        
        # Extract Avro schema
        avro_schema = json.loads(data.meta.get("avro.schema"))
        
        # Read Avro data records
        avro_data = [record for record in data]
        
    return avro_schema, avro_data


# Create table in HBase
# We can add additional parameters to further customize the creation of the tables
def create_or_check_hbase_table(connection, table_name, column_families):
    if table_name.encode() not in connection.tables():
        table_data = {"name": table_name, 
                      "column_schema": [{"name": cf_name, "versions": cf_data["max_versions"]} for cf_name, cf_data in column_families.items()]
                      }
        connection.create_table(
            name=table_data["name"],
            families={cf["name"]: dict(max_versions=cf["versions"]) for cf in table_data["column_schema"]}
        )
        # Print a message indicating that the table was created successfully
        print(f"Table '{table_name}' created successfully.")
    else:
        # Print a message indicating that the table already exists
        print(f"Table '{table_name}' already exists.")


# Insert data into HBase table
def batch_insert_into_hbase_table(hbase_connection, table_name, row_key, data):
    table = hbase_connection.table(table_name)

    cell_data = [{"column": f"{cf}:{q}".encode('utf-8'), "value": base64.b64encode(str(value).encode('utf-8'))}
                 for cf, columns in data.items() for q, value in columns.items()]

    payload = {"row": [{"key": str(row_key).encode('utf-8'), "cell": cell_data}]}

    # Insert data into HBase table using batch operations
    with table.batch() as b:
        for row in payload["row"]:
            key = row["key"]
            cells = row["cell"]
            cell_dict = {cell["column"]: cell["value"] for cell in cells}
            b.put(key, cell_dict)


# Process Avro file and insert data into HBase table
def process_avro_file_and_insert_to_hbase(hdfs_client, hdfs_avro_path, hbase_connection, 
                                      avro_file, table_name, cf_structure):
    # Read Avro file from HDFS
    _, avro_data = read_avro_file_from_hdfs(hdfs_client, hdfs_avro_path, avro_file)

    for record_index, record in tqdm(enumerate(avro_data), total=len(avro_data), desc=f"Inserting data into {table_name}"):
        hbase_data = {cf_name: {q: '' for q in cf_info["qualifiers"]} for cf_name, cf_info in cf_structure["families"].items()}
        row_key = record.get(cf_structure.get("row_id"), record_index)
        for key, value in record.items():
            for cf_name, cf_info in cf_structure["families"].items():
                if key in cf_info["qualifiers"]:
                    hbase_data[cf_name][key] = value if value is not None else ""
                    
        batch_insert_into_hbase_table(hbase_connection, table_name, row_key, hbase_data)


# Function to load HBase tables from Avro files in HDFS
def load_and_create_hbase_tables_from_avro_files(hdfs_client, hdfs_avro_path, hbase_connection):
    # Load HBase tables structure from JSON file
    with open("HBase_tables_structure.json",  encoding="utf-8") as json_file:
        tables_structure = json.load(json_file)

    # Create HBase tables based on the structure defined in the JSON file
    table_names = set(tables_structure.keys())
    for table_name in table_names:  
        create_or_check_hbase_table(hbase_connection, table_name, tables_structure[table_name]["families"])

    # Create a list of strings containing the names of the Avro files in the specified HDFS directory
    avro_files = [file for file in hdfs_client.list(hdfs_avro_path) if file.endswith(".avro")]

    # Process each Avro file and insert data into the corresponding HBase table
    for avro_file in avro_files:
        table_name = avro_file.split('.')[0]
        if table_name in tables_structure:
            print(f"Processing file: {avro_file} for table: {table_name}")
            process_avro_file_and_insert_to_hbase(hdfs_client, hdfs_avro_path, hbase_connection, 
                                              avro_file, table_name, tables_structure[table_name])

# Function to delete a directory in HDFS
def delete_hdfs_directory(hdfs_client, directory):
    hdfs_client.delete(directory, recursive=True)
    print(f"Directory '{directory}' deleted successfully.")


# Main function
def main(temp_dir):
    # Initialize connection to HDFS
    hdfs_client = InsecureClient("http://192.168.1.47:9870", user="bdm")

    # Initialize connection to HBase
    hbase_connection = happybase.Connection(host = "192.168.1.47")

    # Load HBase tables from Avro files in HDFS
    load_and_create_hbase_tables_from_avro_files(hdfs_client, temp_dir, hbase_connection)

    # Close connections
    hbase_connection.close()

    # Delete the directory containing the Avro files in HDFS
    delete_hdfs_directory(hdfs_client, temp_dir)

# Entry point of the script
if __name__ == "__main__":
    temp_dir = sys.argv[1]
    print(temp_dir)
    main(temp_dir)
    print('Runs successfully using terminal!')