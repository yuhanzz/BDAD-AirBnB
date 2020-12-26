# Ingest 311 Service Requests Data
##step1-download data into NYU HPC machine
wget https://data.cityofnewyork.us/api/views/erm2-nwe9/rows.csv?accessType=DOWNLOAD
##step2-rename the data file to nyc311.csv
mv rows.csv?accessType=DOWNLOAD nyc311.csv
##step3-create the HDFS directory for the team project
hdfs dfs -mkdir team_project
##step4-put the data into HDFS
hdfs dfs -put nyc311.csv team_project/





