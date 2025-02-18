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

""" ********* Read JSON file having Nested schema/data and 
# do flatten data using EXPLODE***************"""
#ARRAY#

data="""

{
    "id": 2,
    "trainer": "sai",
    "zeyostudents" : [
    			 "Aarti",
    			 "Arun"    
    ]
}

"""

rdd = sc.parallelize([data])


df = spark.read.option("multiline","true").json(rdd)        # DATAFRAME READS

df.show()
df.printSchema()

flattendf = df.selectExpr(
    "id",
    "trainer",
    "explode(zeyostudents) as students"
)

flattendf.show()
flattendf.printSchema()

#WITH COLUMN#

data="""

{
    "id": 2,
    "trainer": "sai",
    "zeyostudents" : [
    			 "Aarti",
    			 "Arun"    
    ]
}

"""

rdd = sc.parallelize([data])


df = spark.read.option("multiline","true").json(rdd)        # DATAFRAME READS

df.show()
df.printSchema()


from pyspark.sql.functions import *

flattendf = df.withColumn("zeyostudents",expr("explode(zeyostudents)"))

flattendf.show()
flattendf.printSchema()