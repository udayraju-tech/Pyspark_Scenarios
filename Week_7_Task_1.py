from re import split

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys

import os, urllib.request, ssl; ssl_context = ssl._create_unverified_context(); [open(path, 'wb').write(urllib.request.urlopen(url, context=ssl_context).read()) for url, path in { "https://github.com/saiadityaus1/SparkCore1/raw/refs/heads/master/data.orc": "data.orc"}.items()]

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

data_dir1 = "hadoop/bin"
os.makedirs(data_dir1, exist_ok=True)

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path

os.environ['JAVA_HOME'] = r''

conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

# ********* Task 1- SCENARIO***************
print("*********** TASK 1 Scenario**************")
data="""


{
    "id": 2,
    "trainer": "sai",
    "zeyoaddress": {
            "permanentAddress": "hyderabad",
            "temporaryAddress": "chennai"
    }
}


"""

rdd = sc.parallelize([data])       # RDD CONVERSION


df = spark.read.option("multiline","true").json(rdd)        # DATAFRAME READS

df.show()          # second priority
df.printSchema()   # First Priority

flattendf = df.select(
                     "id",
                     "trainer",
                     "zeyoaddress.permanentAddress",
                     "zeyoaddress.temporaryAddress"
)

flattendf.show()
flattendf.printSchema()

# Struct inside struct
data="""


{
    "id": 2,
    "trainer": "sai",
    "zeyoaddress": {
        "user": {
            "permanentAddress": "hyderabad",
            "temporaryAddress": "chennai"
        }
    }
}

"""

# STRUCT DATA FILE

data="""


{
	"place": "Hyderabad",
	"user": {
		"name": "zeyo",
		"address": {
			"number": "40",
			"street": "ashok nagar",
			"pin": "400209"
		}
	}
}


"""

rdd = sc.parallelize([data])       # RDD CONVERSION


df = spark.read.option("multiline","true").json(rdd)        # DATAFRAME READS

df.show()          # second priority
df.printSchema()   # First Priority



flattendf = df.select(

                "place",
                "user.address.number",
                "user.address.pin",
                "user.address.street",
                "user.name"

)

flattendf.show()
flattendf.printSchema()

## WITHCOLUMN Scenario

data = """



{
    "place": "Hyderabad",
    "user": {
        "name": "zeyo",
        "address": {
            "number": "40",
            "street": "ashok nagar",
            "pin": "400209"
        }
    }
}



"""

rdd = sc.parallelize([data])  # RDD CONVERSION

df = spark.read.option("multiline", "true").json(rdd)  # DATAFRAME READS

df.show()  # second priority
df.printSchema()  # First Priority

from pyspark.sql.functions import *

flattendf = (

    df.withColumn("number", expr("user.address.number"))
    .withColumn("pin", expr("user.address.pin"))
    .withColumn("street", expr("user.address.street"))
    .withColumn("name", expr("user.name"))
    .drop("user")

)

flattendf.show()
flattendf.printSchema()

