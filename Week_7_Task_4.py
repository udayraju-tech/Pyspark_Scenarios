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

#ACTORS JSON#


data="""

{
  "country" : "US",
  "version" : "0.6",
  "Actors": [
    {
      "name": "Tom Cruise",
      "age": 56,
      "BornAt": "Syracuse, NY",
      "Birthdate": "July 3, 1962",
      "photo": "https://jsonformatter.org/img/tom-cruise.jpg",
      "wife": null,
      "weight": 67.5,
      "hasChildren": true,
      "hasGreyHair": false,
      "picture": {
                    "large": "https://randomuser.me/api/portraits/men/73.jpg",
                    "medium": "https://randomuser.me/api/portraits/med/men/73.jpg",
                    "thumbnail": "https://randomuser.me/api/portraits/thumb/men/73.jpg"
                }
    },
    {
      "name": "Robert Downey Jr.",
      "age": 53,
      "BornAt": "New York City, NY",
      "Birthdate": "April 4, 1965",
      "photo": "https://jsonformatter.org/img/Robert-Downey-Jr.jpg",
      "wife": "Susan Downey",
      "weight": 77.1,
      "hasChildren": true,
      "hasGreyHair": false,
      "picture": {
                    "large": "https://randomuser.me/api/portraits/men/78.jpg",
                    "medium": "https://randomuser.me/api/portraits/med/men/78.jpg",
                    "thumbnail": "https://randomuser.me/api/portraits/thumb/men/78.jpg"
                }
    }
  ]
}

"""

rdd = sc.parallelize([data])


df = spark.read.option("multiline","true").json(rdd)        # DATAFRAME READS

df.show()
df.printSchema()

from pyspark.sql.functions import *

actorsexplode=df.withColumn("Actors",expr("explode(Actors)"))

actorsexplode.show()
actorsexplode.printSchema()

finalflatten = actorsexplode.select(

                                    "Actors.Birthdate",
                                    "Actors.BornAt",
                                    "Actors.age",
                                    "Actors.hasChildren",
                                    "Actors.hasGreyHair",
                                    "Actors.name",
                                    "Actors.photo",
                                    "Actors.picture.large",
                                    "Actors.picture.medium",
                                    "Actors.picture.thumbnail",
                                    "Actors.weight",
                                    "Actors.wife",
                                    "country",
                                    "version"


)

finalflatten.show()

finalflatten.printSchema()