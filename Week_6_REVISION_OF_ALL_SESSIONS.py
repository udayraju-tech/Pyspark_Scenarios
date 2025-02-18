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

from pyspark.sql.functions import  *
#
# df = spark.read.format("csv").option("mode","failfast").option("header","true").load("usdata.csv")
# df.show()
#
# df.write.format("parquet").mode("overwrite").save("parquetdata")


## FULL REVISION


lisin = [ 1 , 4 , 6 , 7]

rddin = sc.parallelize(lisin)

print("===== RAW RDD =====")

print(rddin.collect())


print()
print("===== ADD RDD =====")

addin  = rddin.map(lambda x : x + 2 )
print(addin.collect())



ls = ["zeyobron" , "zeyo" , "analytics"]

rddstr = sc.parallelize(ls)
print()
print("===== RAW  string RDD =====")
print(rddstr.collect())




filstr = rddstr.filter(lambda x : 'zeyo' in x)
print()
print("===== Filter  string RDD =====")
print(filstr.collect())






file1 = sc.textFile("file1.txt")

gymdata = file1.filter(lambda x : 'Gymnastics' in x)
print()






mapsplit = gymdata.map(lambda x : x.split(","))

from collections import namedtuple

schema = namedtuple('schema',['txnno','txndate','custno','amount','category','product','city','state','spendby'])

schemardd = mapsplit.map(lambda x : schema(x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8]))

prodfilter = schemardd.filter(lambda x : 'Gymnastics' in x.product)

schemadf = prodfilter.toDF()

print()
print("===== schema df=====")
print()

schemadf.show(5)




csvdf = spark.read.format("csv").option("header","true").load("file3.txt")
print()
print("===== csvdf df=====")
print()
csvdf.show(5)




jsondf = spark.read.format("json").load("file4.json")
print()
print("===== jsondf df=====")
print()
jsondf.show(5)


parquetdf = spark.read.load("file5.parquet")
print()
print("===== parquetdf df=====")
print()
parquetdf.show(5)



collist = ['txnno','txndate','custno','amount','category','product','city','state','spendby']


jsondf1 = jsondf.select(*collist)
print()
print("===== jsondf1 new df=====")
print()
jsondf1.show(5)




uniondf = schemadf.union(csvdf).union(jsondf1).union(parquetdf)
print()
print("===== uniondf df=====")
print()
uniondf.show(5)


from pyspark.sql.functions import *

procdf =(

         uniondf.withColumn("txndate",expr("split(txndate,'-')[2]"))
                .withColumnRenamed("txndate","year")
                .withColumn("status",expr("case when spendby='cash' then 1 else 0 end"))
                .filter("txnno > 50000")
)

print()
print("===== procdf df=====")
print()
procdf.show(5)



aggdf = procdf.groupby("category").agg(sum("amount").alias("total")).withColumn("total",expr("cast(total as decimal(18,2))"))

aggdf.show()




data4 = [
    (1, "raj"),
    (2, "ravi"),
    (3, "sai"),
    (5, "rani")
]

df1 = spark.createDataFrame(data4, ["id", "name"])
df1.show()

data3 = [
    (1, "mouse"),
    (3, "mobile"),
    (7, "laptop")
]

df2 = spark.createDataFrame(data3, ["id", "product"])
df2.show()


innerjoin = df1.join(  df2  ,  ["id"]  ,  "inner")
innerjoin.show()




leftjoin  = df1.join( df2 , ["id"] , "left").orderBy("id")
leftjoin.show()



rightjoin = df1.join( df2 ,["id"] , "right").orderBy("id")
rightjoin.show()



fulljoin = df1.join(df2, ["id"], "full").orderBy("id")
fulljoin.show()



crossjoin=df1.crossJoin(df2)
crossjoin.show()



leftanti = df1.join(df2,["id"],"left_anti")
leftanti.show()



from pyspark.sql.functions import  *


data = [("DEPT3", 500),
        ("DEPT3", 200),
        ("DEPT1", 1000),
        ("DEPT1", 700),
        ("DEPT1", 700),
        ("DEPT1", 500),
        ("DEPT2", 400),
        ("DEPT2", 200)]
columns = ["dept", "salary"]
df = spark.createDataFrame(data, columns)
df.show()

# 🔴🔴 STEP 1  CREATE WINDOW ON DEPT WITH DESC ORDER OF SALARY

from  pyspark.sql.window import Window
deptwindow = Window.partitionBy("dept").orderBy(col("salary").desc())

# 🔴🔴 STEP 2  Applying window on DF with dense rank #########

dfrank = df.withColumn("drank",dense_rank().over(deptwindow))
dfrank.show()

# 🔴🔴 Step 3 Filter Rank 2 and Drop drank #####

finaldf = dfrank.filter("drank=2").drop("drank")
finaldf.show()