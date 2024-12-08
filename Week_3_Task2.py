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


print("========= DATA PREPARATION======")

data = [
    ["00000001", "05-26-2011", 300, "Exercise", "Weightlifting", "credit"],
    ["00000002", "06-01-2011", 100, "Exercise", "GymnasticsPro", "cash"],
    ["00000003", "06-05-2011", 100, "Gymnastics", "Rings", "credit"],
    ["00000004", "12-17-2011", 300, "Team Sports", "Field", "paytm"],
    ["00000005", "02-14-2011", 200, "Gymnastics", None, "cash"]
]

df = spark.createDataFrame(data, ["tno","tdate","amount","category","product","spendby"])

seldf = df.select("tno", "tdate")
seldf.show()

dropdf = df.drop("tno", "tdate")
dropdf.show()

mulcolumn = df.filter("category='Exercise' or spendby='cash'")
mulcolumn.show()

mulSel = df.filter("category in ('Exercise', 'Gymnastics')")
mulSel.show()

mulProdNull = df.filter("product is null")
mulProdNull.show()

ProdNotNull = df.filter("product is not null")
ProdNotNull.show()

