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
#  CHILD,GRAND PARENTS,PARENTS SCENARIO

data = [("A", "AA"), ("B", "BB"), ("C", "CC"), ("AA", "AAA"), ("BB", "BBB"), ("CC", "CCC")]

df = spark.createDataFrame(data, ["child", "parent"])
df.show()


df1  = df

df2  = df.withColumnRenamed("child","child1").withColumnRenamed("parent","parent1")


df1.show()
df2.show()


joindf = df1.join(df2, df1["child"]==df2["parent1"] ,"inner")
joindf.show()



finaldf = (

            joindf
            .drop("parent1")

)
finaldf.show()


dffinal =(
        finaldf.withColumnRenamed("parent","parent1")
               .withColumnRenamed("child","parent")
                .withColumnRenamed("child1","child")
                .withColumnRenamed("parent1","GrandParent")
                .select("child","parent","GrandParent")
)

dffinal.show()