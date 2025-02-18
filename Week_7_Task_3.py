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
## URL DATA FULL CODE##

import os
import urllib.request
import ssl

urldata=(
    urllib.request

    .urlopen(
             "https://randomuser.me/api/0.8/?results=10",
             context=ssl._create_unverified_context()
             )
    .read()
    .decode('utf-8')
)
### PYSPARK

df = spark.read.json(sc.parallelize([urldata]))
df.show()
df.printSchema()

from pyspark.sql.functions import *
resultexp = df.withColumn("results",expr("explode(results)"))
resultexp.show()
resultexp.printSchema()

finalflatten =  resultexp.select(
                                "nationality",
                                "results.user.cell",
                                "results.user.dob",
                                "results.user.email",
                                "results.user.gender",
                                "results.user.location.city",
                                "results.user.location.state",
                                "results.user.location.street",
                                "results.user.location.zip",
                                "results.user.md5",
                                "results.user.name.first",
                                "results.user.name.last",
                                "results.user.name.title",
                                "results.user.password",
                                "results.user.phone",
                                "results.user.picture.large",
                                "results.user.picture.medium",
                                "results.user.picture.thumbnail",
                                "results.user.registered",
                                "results.user.salt",
                                "results.user.sha1",
                                "results.user.sha256",
                                "results.user.username",
                                "seed",
                                "version"

)

finalflatten.show()
finalflatten.printSchema()