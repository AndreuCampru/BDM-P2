#!/bin/bash

# Define the base directory for the data
# Set BASEDIR1 to the path where the data is stored
BASEDIR1='C:/Users/Pablo/Desktop/Master/2nd Semester/BDM-Big Data Management/Project2/Code for project 1/data'
#BASEDIR1='C:/Users/andre/Documents/Master in Data Science/First year/Second semester/Subjects/Big Data Management/Project/D1/Semester 2/BDM/P1 - Julian Fransen and Andreu Camprubi/data'

BASEDIR2='/user/bdm/temporal_landing'

# pip install hdfs
# pip install feedparser
# pip install fastavro

echo "Run data_collectors.py script to collect data and flush to temporal landing..."
python data_collectors.py "$BASEDIR1"

echo "Run data_persistence_loader.py script to fetch data from temporal and load into HBase for the persistent landing..."
# Extract relative path from BASEDIR2
RELATIVE_BASEDIR2=${BASEDIR2##*/}

python data_persistence_loader.py "$RELATIVE_BASEDIR2"
