
## Acknowledgment:

### Analysis: 
    spark MLLib Guide(https://spark.apache.org/docs/2.3.0/ml-guide.html)

### Html Templates: 
    https://speckyboy.com/code-snippet-form-ui/ (used this as a reference for the form style and made adjustments)

### How to serve a Spark MLib model with flask:
    https://stackoverflow.com/questions/40715458/loading-a-pyspark-ml-model-in-a-non-spark-environment

## Team Works:
  Lillian Huang: frontend + 1/3 analytics (AirBnb) + hypertuning the parameters for the models
  Yuhan Zhou: machine learning model + 1/3 analytics (Real Estate)  + hypertuning the parameters for the models 
  Muxin Xu: backend + 1/3 analytics (311 Complaint) + hypertuning the parameters for the models

## Tools and Environment:
  Scala version 2.11.8 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_162)
  Spark 2.3.0
  Python 2.7.13
  Linux Red Hat 4.4.7-23

  build web application:
    flask, jinja2
  build machine learning model:
    Spark SQL, Spark MLlib

## Analytics and Inferences

### Datasets:

| NAME | RAW DATA LOCATION ON DUMBO |
| ------ | --------- |
| NYC 311 Complaints | `hdfs:///user/lh2978/project/data/listings/datasets/clean_nyc311` |
| NYC Estate Prices | `hdfs:///user/lh2978/project/data/listings/datasets/real_estate_cleaned_data`|
| NYC AirBnb listings | `hdfs:///user/lh2978/project/data/listings/datasets/combined_header`|

### Analytics and Inferences on Datasets

| NAME | Brief Summary |
| ------ | --------- |
| NYC 311 Complaints | `The complaint frequency is relatively high in Brooklyn, Southern Queens and low in Manhattan and northeastern Queens. The correlation between the complaint frequency and the AirBnB listing prices were negative showing that the higher the frequency of complaints tended to portray a lower AirBnB listing price. ` |
| NYC Estate Prices | `The real estate price is relatively high in Manhattan and low in Bronx and middle the of Brooklyn. The correlation between the Estate Prices and AirBnB listing prices were positive thus showing that the higher the estate price the higher the AirBnB listing price tended to be. ` |
| NYC AirBnb listings | `The maximum prices of listings based on zipcode were fairly higher in Manhattan and parts of Brooklyn and lower in the Queens area. The average prices of the listings based on zipcode showed that most areas other than Manhattan and some parts of Brooklyn had roughly the same price level. From our Random Forest Regression Model, we noticed that field such as room_is_entire_room,room_is_private_room,cleaning_fee,etc were all of high importance in predicting the price of the listing.    ` |

## Code Directories and Files:

```
├── act_rem_code              
│   ├── server.py
│   ├── static
│   │     ├── images
│   └── templates
│         ├── About.html
│         ├── HomePage.html
│         ├── PredictForm.html
│         ├── Result.html|
├── app_code
│   ├── correlations
│   │    ├── pom.xml
│   │    ├── src
│   │    │   ├── main
│   │    │       ├── scala
│   │    │           ├── correlation.scala
│   │    ├── target
│   └── predictions
│       ├── pom.xml
│       ├── src
│       │   ├── main
│       │       ├── scala
│       │           ├── train.scala
│       ├── target
│
├── data_ingest
│   ├── data_ingestAirBnB.txt
│   ├── ingest-nyc311data.md  
│   ├── ingestRealEstate.sh
├── etl_code
│   ├── Clean311.scalaspark
│   ├── cleanAirBnb.scalaspark
│   ├── CleanRealEstate.scalaspark
│   ├── Preprocess_xls_to_csvRealEstate.scalaspark
├── profiling_code
│   ├── Profiling311.scalaspark
│   ├── profilingAirBnb.scalaspark
│   ├── ProﬁlingRealEstate.scalaspark
├── screenshots
└── Readme.md
```
## How to ingest data

### How to ingest real estate data:
cd data_ingest
./ingestRealEstate.sh

### how to ingest 311 complaint data:
please read ./data_ingest/ingest-nyc311data.md and follow the instructions

### how to ingest AirBnb data:
please read ./data_ingest/data_ingestAirBnB.txt and follow the instructions

## How to clean data

### How to clean real estate data:
run the commands in etl_code/Preprocess_xls_to_csvRealEstate.scalaspark in spark2-shell
run the commands in etl_code/CleanRealEstate.scalaspark in spark2-shell

### How to clean 311 complaint data:

run the commands in etl_code/Clean311.scalaspark in spark2-shell

### How to clean AirBnb data:

run the commands in etl_code/cleanAirBnb.scalaspark in spark2-shell


## How to profile data

### How to profile real estate data:
 run the commands in profiling_code/ProﬁlingRealEstate.scalaspark in spark2-shell

### How to profile 311 complaint data:
 run the commands in profiling_code/Profiling311.scalaspark in spark2-shell

### How to profile AirBnb data:
 run the commands in profiling_code/profilingAirBnb.scalaspark in spark2-shell 

## How to run correlations:
cd app_code/correlations
module load maven/3.5.2
mvn clean install
spark2-submit --class Correlation target/AirbnbCorrelation-0.0.1.jar

## How to run predictions:
cd app_code/predictions
module load maven/3.5.2
mvn clean install
spark2-submit --class TrainModel target/AirbnbPrediction-0.0.1.jar

## How to run the web services:

### run the following commands in dumbo:
source /home/mx623/project_env/bin/activate
export SPARK_HOME=/opt/cloudera/parcels/SPARK2-2.3.0.cloudera4-1.cdh5.13.3.p0.611179/lib/spark2/
cd act_rem_code/

python server.py host_ip_address host_running_port 
// e.g. python server.py 0.0.0.0 8080

### start your VPN and run the following command in your local machine:
SSH -CfN -L 0.0.0.0:8080:host_ip_address:host_running_port netid@dumbo.hpc.nyu.edu

// visit the website with 0.0.0.0:8080
// 0.0.0.0:8080/home
// 0.0.0.0:8080/about
// 0.0.0.0:8080/predict


