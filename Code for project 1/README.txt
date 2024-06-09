1. Run this code to configure the VM: 
===============================================================
# Code to execute to format HDFS (start from scratch)

/home/bdm/BDM_Software/hadoop/sbin/stop-dfs.sh

cd /home/bdm/BDM_Software

rm -rf data

˜ /BDM_Software/hadoop/bin/hdfs namenode -format

/home/bdm/BDM_Software/hadoop/sbin/start-dfs.sh

˜ /BDM_Software/hadoop/bin/hdfs dfs -mkdir /user

˜ /BDM_Software/hadoop/bin/hdfs dfs -mkdir /user/bdm

˜ /BDM_Software/hadoop/bin/hdfs dfs -chmod -R 777 /user/bdm/

# Code to be able to use the Thrift server in HBase

˜ /BDM_Software/hbase/bin/start-hbase.sh

~/BDM_Software/hbase/bin/hbase-daemon.sh start thrift
===============================================================
2. Open run.sh, change BASEDIR1 to the directory that contains the data
3. Run run.sh in terminal (i.e. ./run.sh)