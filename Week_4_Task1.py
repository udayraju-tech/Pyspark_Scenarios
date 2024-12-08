from re import split

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys

from pyspark.sql.functions import *

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

data = [
    ("00000000", "06-26-2011", 200, "Exercise", "GymnasticsPro", "cash"),
    ("00000001", "05-26-2011", 300, "Exercise", "Weightlifting", "credit"),
    ("00000002", "06-01-2011", 100, "Exercise", "GymnasticsPro", "cash"),
    ("00000003", "06-05-2011", 100, "Gymnastics", "Rings", "credit"),
    ("00000004", "12-17-2011", 300, "Team Sports", "Field", "paytm"),
    ("00000005", "02-14-2011", 200, "Gymnastics", None, "cash")
]

df = spark.createDataFrame(data).toDF("tno", "tdate", "amount", "category", "product", "spendby")

df.show()

# mulSel_df= df.filter("category is 'Exercise' and product is 'GymnasticsPro' ")
# mulSel_df.show()

LikeOperSearch = df.filter("product like '%Gymna%' ")
LikeOperSearch.show();

NotLikeOperSearch = df.filter("product not like '%Gymna%' ")
NotLikeOperSearch.show();

nullProdNull= df.filter("product is null")
nullProdNull.show()

notNullProd = df.filter("product is not null")
notNullProd.show()

NotCatgerogy = df.filter("category != 'Exercise' ")
NotCatgerogy.show()

procdf = df.selectExpr(

                    "tno",  #Column
                    "tdate",   #Column
                    "amount", #Column
                    "upper(category) as category ",  # Expression
                    "product", #Column
                    "spendby" #Column

)

procdf.show()

procdf = df.selectExpr(
  "cast(tno as int) as tno",
  "split(tdate, '-') [2] as tdate",
  "amount+100 as amount",
  "upper(category) as category",
  "concat(product, '~zeyo') as product",
  "spendby",
  """case
    when spendby='cash' then 0
    when spendby='paytm' then 2
    else 1
    end as status
  """
)

procdf.show()

withcolexp = (
  df.withColumn("category", expr("upper(category)"))
     .withColumn("product", expr("concat(product, '~Zeyo')"))
     .withColumn("amount", expr("amount+100"))
     .withColumn("tno", expr("cast(tno as int)"))
     .withColumn("tdate", expr("split(tdate, '-') [2]"))
     .withColumn("status", expr("case when spendby='cash' then 0 when spendby='paytm' then 2 else 1 end"))
     .withColumnRenamed("tdate", "year")
)

withcolexp.show()