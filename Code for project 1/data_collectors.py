import os
import pandas as pd
from fastavro import parse_schema
import feedparser
import sys

from hdfs import InsecureClient
from hdfs.ext.avro import AvroWriter


# Define the default url
DEFAULT_URL = "https://data.europa.eu/api/hub/search/en/feeds/datasets/https-opendata-ajuntament-barcelona-cat-data-dataset-est-densitat.rss"


# Function to load data from an external source
def load_external_source(source_url = DEFAULT_URL, verbose = False):
    feed = feedparser.parse(source_url)
    dict_of_dfs = {}
    count = 1
    for entry in feed.entries:
      for link in entry.links:
        if link.type == 'csv':
          url = link.href
          df = pd.read_csv(url)
          dict_of_dfs[f'df_{count}'] = df
          count+=1
    # just to check
    if verbose:
      for item in dict_of_dfs:
        print(dict_of_dfs[item].head())
    return dict_of_dfs


# Function to load JSON files from a directory
def load_json(directory):
    json_files = [f for f in os.listdir(directory) if f.endswith('.json')]
    dataframes = {}
    for i, file in enumerate(json_files):
        df_name = f'df_{i+1}'
        file_path = os.path.join(directory, file)
        dataframes[df_name] = pd.read_json(file_path)
    return dataframes


# Function to merge multiple dataframes into a single dataframe
def get_df_from_dict(dictionary_of_dfs):
    df_list = []
    for _, df in dictionary_of_dfs.items():
        df_list.append(df)
    df_merged = pd.concat(df_list, ignore_index=True)
    return df_merged


# Function to get the schema of a dataframe
def get_schema_df(df, file_name):
    x = df.dtypes
    y = df.columns
    new_x = []
    for t in x:
      n = str(t)
      new_x.append(n)
    new_x = ['int' if 'int' in s else s for s in new_x]
    new_x = ['float' if 'float' in s else s for s in new_x]
    new_x = ['datetime' if 'datetime' in s else s for s in new_x]
    new_x = ['boolean' if 'bool' in s else s for s in new_x]
    x = ['string' if 'object' in s else s for s in new_x]
    fields = []
    for index, val in enumerate(x):
      name = y[index]
      typ = x[index]
      dict_c = {'name' : name, 'type': ["null", f"{typ}"]}
      fields.append(dict_c)
    schema = {
      'name': file_name,
      'type': 'record',
      'fields': fields}
    parsed_schema = parse_schema(schema)
    return parsed_schema


# Function to get a dataframe from a CSV file
def get_df_csv(csv_file, path):
    os.chdir(path)
    df = pd.read_csv(csv_file)
    return df


# Function to write a dataframe to an Avro file on HDFS
def df_to_avro_hdfs(schema, df, file_name, client_hdfs):
    for field in schema['fields']:
        column_name = field['name']
        column_type = field['type'][1]  # Extracting the Avro type from the schema
        if column_type == 'string':
            df[column_name] = df[column_name].astype(str)
        elif column_type == 'int':
            df[column_name] = df[column_name].astype(int)
        elif column_type == 'float':
            df[column_name] = df[column_name].astype(float)
        elif column_type == 'boolean':
            df[column_name] = df[column_name].astype(bool)
        elif column_type == 'datetime':
            df[column_name] = pd.to_datetime(df[column_name])

    records = df.to_dict('records')
    
    # Create AvroWriter instance to write Avro data to HDFS
    with AvroWriter(client_hdfs, file_name) as writer:
        for record in records:
            writer.write(record)
    print('Written to Avro file on HDFS!')


# Function to load CSV files from a directory
def load_csv(folder_path):
    csv_files = []
    for file in os.listdir(folder_path):
        if file.endswith(f'.csv'):
            csv_files.append(file)
    print("CSV Files Found: " + str(csv_files) + "\n")
    return csv_files

# Main function
def main(hdir):
    #define dirs
    HDIR = hdir #'C:/Users/andre/Documents/Master in Data Science/First year/Second semester/Subjects/Big Data Management/Project/D1/Semester 2/BDM/P1 - Julian Fransen and Andreu Camprubi/data'
    I_DIR = HDIR + '/idealista'
    LT_DIR = HDIR + '/lookup_tables'
    BCN_INC_DIR = HDIR + '/opendatabcn-income'
    
    # Initialize connection to HDFS
    client_hdfs = InsecureClient('http://192.168.1.47:9870', user='bdm')
    # Load CSV files from BCN_INC_DIR folder
    csv_files_bcn = load_csv(BCN_INC_DIR)
    merged_df_bcn = pd.DataFrame()  # Initialize an empty DataFrame to merge all BCN_INC_DIR CSV data
    
    
    # Iterate through each CSV file in BCN_INC_DIR, read it into a DataFrame, and merge it with merged_df_bcn
    for f in csv_files_bcn:
        df = get_df_csv(f, BCN_INC_DIR)
        merged_df_bcn = pd.concat([merged_df_bcn, df], ignore_index=True)

    # Get the schema for the merged DataFrame from BCN_INC_DIR
    file_name_bcn = 'bcn_inc_data'
    schema_bcn = get_schema_df(merged_df_bcn, file_name_bcn)
    
    # Write the merged DataFrame from BCN_INC_DIR to a single Avro file on HDFS
    df_to_avro_hdfs(schema_bcn, merged_df_bcn, 'temporal_landing/' + file_name_bcn + '.avro', client_hdfs)

   # For lookup tables in csv format
    print('loading from csv folder...')
    dirs = [LT_DIR] 
    for d in dirs:
      csv_files = load_csv(d)
      for f in csv_files:
        file_name = str(f)[:-4]
        df = get_df_csv(f,d)
        schema = get_schema_df(df, file_name)
        df_to_avro_hdfs(schema, df, 'temporal_landing/' + file_name + '.avro', client_hdfs)

    # Load JSON files from idealista folder and write them to Avro files
    print('loading from json folder...')
    file_name_json = 'jsons_from_idealista'
    json_files_dict = load_json(I_DIR)
    df_json = get_df_from_dict(json_files_dict)
    schema_json = get_schema_df(df_json, file_name_json)
    df_to_avro_hdfs(schema_json, df_json, 'temporal_landing/' + file_name_json + '.avro', client_hdfs)

    # Load data from the external source and write it to an Avro file
    print('loading from external source...')
    file_name_ext = 'files_from_external_source'
    ext_dict = load_external_source()
    df_ext = get_df_from_dict(ext_dict)
    schema_ext = get_schema_df(df_ext, file_name_ext)
    df_to_avro_hdfs(schema_ext, df_ext, 'temporal_landing/' + file_name_ext + '.avro', client_hdfs)

# Execute the main function
if __name__ == "__main__":
    datadir = sys.argv[1]
    main(datadir)
    print('Runs successfully using terminal!')