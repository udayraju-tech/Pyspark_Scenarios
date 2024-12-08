
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


# Other Details... ther way

# PREP DATA
data = [
    (0, "06-26-2011", 300.4, "Exercise", "GymnasticsPro", "cash"),
    (1, "05-26-2011", 200.0, "Exercise Band", "Weightlifting", "credit"),
    (2, "06-01-2011", 300.4, "Exercise", "Gymnastics Pro", "cash"),
    (3, "06-05-2011", 100.0, "Gymnastics", "Rings", "credit"),
    (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (5, "02-14-2011", 200.0, "Gymnastics", None, "cash"),
    (6, "06-05-2011", 100.0, "Exercise", "Rings", "credit"),
    (7, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (8, "02-14-2011", 200.0, "Gymnastics", None, "cash")
]

df = spark.createDataFrame(data, ["id", "tdate", "amount", "category", "product", "spendby"])
df.show()





data2 = [
    (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (5, "02-14-2011", 200.0, "Gymnastics", None, "cash"),
    (6, "02-14-2011", 200.0, "Winter", None, "cash"),
    (7, "02-14-2011", 200.0, "Winter", None, "cash")
]

df1 = spark.createDataFrame(data2, ["id", "tdate", "amount", "category", "product", "spendby"])
df1.show()






data4 = [
    (1, "raj"),
    (2, "ravi"),
    (3, "sai"),
    (5, "rani")
]



cust = spark.createDataFrame(data4, ["id", "name"])
cust.show()

data3 = [
    (1, "mouse"),
    (3, "mobile"),
    (7, "laptop")
]

prod = spark.createDataFrame(data3, ["id", "product"])
prod.show()


# Register DataFrames as temporary views
df.createOrReplaceTempView("df")
df1.createOrReplaceTempView("df1")
cust.createOrReplaceTempView("cust")
prod.createOrReplaceTempView("prod")

print("=========DONE GOOD TO GO BELOW======")


spark.sql("select * from df  ").show()
spark.sql("select id,tdate from df  order by id").show()
spark.sql("select id,tdate,category from df   where category='Exercise'   order by id").show()
spark.sql("select id,tdate,category,spendby from df   where category='Exercise' and spendby='cash' ").show()
spark.sql("select id,tdate from df   where category='Exercise' and spendby='cash' ").show()

spark.sql("select id,tdate from df   where category='Exercise' and spendby='cash' ").show()
spark.sql("select * from df   where category in ('Exercise','Gymnastics')").show()

spark.sql("select * from df   where product like ('%Gymnastics%')").show()
spark.sql("select * from df   where category != 'Exercise'").show()
spark.sql("select * from df   where category not in ('Exercise','Gymnastics')").show()

spark.sql("select * from df   where product is null").show()
spark.sql("select max(id) from df   ").show()
spark.sql("select min(id) from df   ").show()


spark.sql("select count(1) from df   ").show()

spark.sql("select *,case when spendby='cash' then 1 else 0 end  as status from df  ").show()
spark.sql("select concat(id,'-',category) as concat  from df  ").show()
spark.sql("select concat_ws('-',id,category,product) as concat   from df    ").show()


spark.sql("select category,lower(category) as lower  from df ").show()

spark.sql("select amount,ceil(amount) from df").show()
spark.sql("select amount,round(amount) from df").show()
spark.sql("select coalesce(product,'NA') from df").show()
spark.sql("select trim(product) from df").show()

spark.sql("select distinct (category,spendby) from df").show()
spark.sql("select substring(trim(product),1,10) from df").show()
spark.sql("select SUBSTRING_INDEX(category,' ',1) as spl from df").show()
spark.sql("select * from df union all select * from df1").show()
spark.sql("select * from df union select * from df1 order by id").show()

spark.sql("select category, sum(amount) as total from df group by category").show()
spark.sql("select category,spendby,sum(amount)  as total from df group by category,spendby").show()
spark.sql("select category,spendby,sum(amount) As total,count(amount)  as cnt from df group by category,spendby").show()
spark.sql("select category, max(amount) as max from df group by category").show()
spark.sql("select category, max(amount) as max from df group by category order by category").show()
spark.sql("select category, max(amount) as max from df group by category order by category desc").show()


spark.sql("SELECT category,amount, row_number() OVER ( partition by category order by amount desc ) AS row_number FROM df").show()
spark.sql("SELECT category,amount, dense_rank() OVER ( partition by category order by amount desc ) AS dense_rank FROM df").show()
spark.sql("SELECT category,amount, rank() OVER ( partition by category order by amount desc ) AS rank FROM df").show()
spark.sql("SELECT category,amount, lead(amount) OVER ( partition by category order by amount desc ) AS lead FROM df").show()
spark.sql("SELECT category,amount, lag(amount) OVER ( partition by category order by amount desc ) AS lag FROM df").show()
spark.sql("select category,count(category) as cnt from df group by category having count(category)>1").show()

spark.sql("select a.id,a.name,b.product from cust a join prod b on a.id=b.id").show()
spark.sql("select a.id,a.name,b.product from cust a left join prod b on a.id=b.id").show()
spark.sql("select a.id,a.name,b.product from cust a right join prod b on a.id=b.id").show()
spark.sql("select a.id,a.name,b.product from cust a full join prod b on a.id=b.id").show()
spark.sql("select a.id,a.name from cust a LEFT ANTI JOIN  prod b on a.id=b.id").show()
spark.sql("select a.id,a.name from cust a LEFT SEMI JOIN  prod b on a.id=b.id").show()

spark.sql("select id,tdate,from_unixtime(unix_timestamp(tdate,'MM-dd-yyyy'),'yyyy-MM-dd') as con_date from df").show()

