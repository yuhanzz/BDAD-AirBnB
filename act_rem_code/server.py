import findspark
findspark.init()
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.ml import PipelineModel
from pyspark.sql import SQLContext
# loading spark context
sc = SparkContext.getOrCreate(SparkConf().setAppName("Referenced Price").set("spark.driver.allowMultipleContexts", "true"))
print('context loaded')
# loading context for sql operations
spark  = SQLContext(sc)
print('spark sql loaded')
# loading data schema from sample data to encapsulate the input data from webpage
SCHEMA = spark.read.csv("hdfs:///user/yz6238/rdba_project/sample_data2", header=True, inferSchema=True).schema
# joint_complaint_estate data, extract relevant data by "yyyy-mm|zipcode"
complaint_estate_data = spark.read.csv("hdfs:///user/lh2978/project/data/listings/datasets/311_estate_combined", header=True, inferSchema=True)
print('schema2 loaded.')
#d = [1,2,3]
#data = sc.parallelize(d)
#print('data parallelized')

# loading trained model from specific path
model = PipelineModel.load("hdfs:///user/lh2978/project/data/listings/datasets/random_forest_model")
print('model loaded.')

# transform some features into one-hot vector
borough_type = {
        "Brooklyn": [1, 0, 0, 0, 0],
        "Staten Island": [0, 1, 0, 0, 0],
        "Manhattan": [0, 0, 1, 0, 0],
        "Bronx": [0, 0, 0, 1, 0],
        "Queens": [0, 0, 0, 0, 1]
    }
property_type = {
        "Hotel":             [1, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        "House":             [0, 1, 0, 0, 0, 0, 0, 0, 0, 0],
        "Apartment":         [0, 0, 1, 0, 0, 0, 0, 0, 0, 0],
        "Guest suite":       [0, 0, 0, 1, 0, 0, 0, 0, 0, 0],
        "Condominium":       [0, 0, 0, 0, 1, 0, 0, 0, 0, 0],
        "Townhouse":         [0, 0, 0, 0, 0, 1, 0, 0, 0, 0],
        "Boutique hotel":    [0, 0, 0, 0, 0, 0, 1, 0, 0, 0],
        "Loft":              [0, 0, 0, 0, 0, 0, 0, 1, 0, 0],
        "Serviced apartment":[0, 0, 0, 0, 0, 0, 0, 0, 1, 0],
        "others":            [0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
    }
room_type = {
        "Private Room":   [1, 0, 0, 0],
        "Entire home/apt":[0, 1, 0, 0],
        "Shared Room":    [0, 0, 1, 0],
        "Hotel Room":     [0, 0, 0, 1]
    }

# get predicted result with data from webpage from trained model
def get_estimated_result(user_info):
    #data = [(479, 53, 92, 35, 23, 17, 0, 18, 25, 8, 0, 781.5692, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 4, 1.0, 1.0, 1.0, 199.0)]
    data = []
    # transform date and zipcode into required form to extract complaint and estate data
    date = user_info["date"]
    date = date[0:len(date)-3]
    zipcode = user_info["zip"]
    key = date +"|"+str(zipcode)

    res = list(complaint_estate_data.filter(complaint_estate_data.key == key).collect())
    # some key date|zipcode may not exist in our database, so it is necessage to check whether we have the required record
    if len(res) == 0:
        return "Invalid Input. No record for " + key +"."

    complaints_estate_row = list(complaint_estate_data.filter(complaint_estate_data.key == key).collect()[0])[1:]
    data.extend(complaints_estate_row)
    print(data)

    borough_is = borough_type[user_info['borough-form']]
    property_is = property_type[user_info['property_type']]
    room_is = room_type[user_info['room_type']]

    data.extend(borough_is)
    data.extend(property_is)
    data.extend(room_is)
    print(data)

    accommodates = int(user_info["accommodates"])
    bathrooms = float(user_info["bathrooms"])
    bedrooms = float(user_info["bedrooms"])
    beds = float(user_info["beds"])
    cleaning_fee = float(user_info["cleaning_fee"])
    host_listings = int(user_info["host_listings_count"])
    availability = int(user_info["availability_90"])
    extra_people = float(user_info["extra_people"])
    security_deposit = float(user_info["security_deposit"])
    minimum_nights = int(user_info["minimum_nights"])
   
    temp = [accommodates, bathrooms, bedrooms, beds, 0.0, cleaning_fee, host_listings, availability, extra_people, security_deposit, minimum_nights]
    # create RDD from memory
    data.extend(temp)
    print(data)
    data = [tuple(data)]
    print(data)
    d = sc.parallelize(data)
    # transform RDD into DF according to required schema
    mydf = spark.createDataFrame(d,schema=SCHEMA)
    # get predicted result
    result = model.transform(mydf).take(1)[0][-1]

    return "Estimated Price is " + str(result) + "."

from flask import Flask, request, render_template
import sys
app = Flask(__name__)

# test connetction
@app.route('/')
def hello():
    return 'Hello From Server!'
# home page of the app
@app.route('/home')
def index():
    return render_template('HomePage.html')
# bried description of the app
@app.route('/about')
def about():
    return render_template('About.html')
# in this page, users are required to submit relevant information to get referenced price
@app.route('/predict', methods=['GET', 'POST'])
def predict():
    if request.method == 'GET':
        return render_template('PredictForm.html')
    user_info = request.form.to_dict()
    print(user_info)
    return render_template('Result.html', price=get_estimated_result(user_info))

# python server.py host_ip_address running_port
app.run(host=sys.argv[1], port=int(sys.argv[2]), debug=True)
