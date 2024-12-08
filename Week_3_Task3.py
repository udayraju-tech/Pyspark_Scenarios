from re import split

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys

import os, urllib.request, ssl;

ssl_context = ssl._create_unverified_context();
[open(path, 'wb').write(urllib.request.urlopen(url, context=ssl_context).read()) for url, path in { "https://github.com/saiadityaus1/SparkCore1/raw/refs/heads/master/data.orc": "data.orc"}.items()]

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
    (0, "06-26-2011", 300.4, "Exercise", "GymnasticsPro", "cash"),
    (1, "05-26-2011", 200.0, "Exercise Band", "Weightlifting!", "credit"),
    (2, "06-01-2011", 300.4, "Exercise", "Gymnastics Pro", "cash"),
    (3, "06-05-2011", 100.0, "Gymnastics", "Rings", "credit"),
    (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (5, "02-14-2011", 200.7, "Gymnastics", None, "cash"),
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

# print("=========DONE GOOD TO GO BELOW======")
#
# print("----- Single Column Filter ----")

# spark.sql("select * from df where category='Exercise'").show()
#
# print("----- MULTI Column Filter ----")
# spark.sql("select id,tdate,category,spendby from df where category='Exercise' and spendby = 'cash'").show()
#
#
# print("----- CONTAINS Filter ----")
#
# spark.sql("select * from df where product like ( '%Gymnastics%')").show()
#
# print("-----NOT CONTAINS Filter ----")
#
# spark.sql("select * from df where category  != ( 'Exercise')").show()
#
# print("-----NOT CONTAINS for multi Filter ----")
#
# spark.sql("select * from df where category  not in ( 'Exercise','Gymnastics')").show()
#
# print("-----IS NULL Filter ----")
#
# spark.sql("select * from df where product is null").show()
#
# spark.sql("select * from df where product is not null").show()
#
#
#
# print("-----IS NOT NULL Filter ----")
#
# spark.sql("select * from df where product is not null").show()
#
# print("-----MAX ID Filter ----")

# spark.sql("select id,tdate,product,amount max(id) from df ").show()
print("-----Order by Filter ----")
spark.sql("select * from df order by amount desc limit 1").show()



# spark.sql("select * from df order by amount desc limit 2, 2").show()
print("-----Order by Filter 2222 ----")
spark.sql("SELECT * FROM (SELECT *, ROW_NUMBER() OVER (ORDER BY amount) AS row_num FROM df) AS numbered_table WHERE row_num BETWEEN 2 AND 3").show()


print("-----MIN ID Filter ----")

spark.sql("select min(id) from df ").show()

print("-----COUNT NO OF ROWS ----")

spark.sql("select count(*) from df ").show()

print("-----STATUS CONDITION CASH '1' CREDIT '0' ----")

spark.sql("select *,Case when spendby = 'cash' then 1 else 0 end from df ").show()

print("-----CONCAT 2 columns ----")

spark.sql("select id,category,concat(id,'-',category) as combination from df ").show()

spark.sql("select concat(id,'-',category) from df ").show()


print("-----CONCAT 3 columns ----")

spark.sql("select id,category,product,concat(id,'-',category,'-',product) from df ").show()

print("-----CONCAT 3 columns using ConcatWS ----")

spark.sql("select id,category,product,concat_ws('-',id,category,product) as concol from df ").show()

print("-----LOWER CASE ----")

spark.sql("select category,lower(category) as low_category from df ").show()

print("-----UPPER CASE **** ----")

spark.sql("select id, tdate, amount, product, spendby, upper(category) as low_category from df").show()

print("-----CEILING ----")

spark.sql("select amount,ceil(amount) from df ").show()


print("-----ROUND ----")

spark.sql("select amount,round(amount) from df ").show()

print("-----REPLACE NULLs ----")

spark.sql("select product, coalesce(product,'NA') as Nullreplace from df ").show()

print("-----TRIM ----")

spark.sql("select trim(product) from df ").show()

print("-----DISTINCT ----")

spark.sql("select distinct category from df ").show()

print("-----SUBSTRING ----")

spark.sql("select substring(product,1,10) from df ").show()

print("-----SPLIT ----")

spark.sql("select product,split(product,' ')[0] as Split_product from df ").show()

# print("-----UNION ALL ----")
#
# spark.sql("select * from df union all select * from df1").show()
#
# print("-----UNION ----")
#
# spark.sql("select * from df union  select * from df1").show()

print("-----AGG SUM ----")

spark.sql("select sum(amount) as sum_amt,category from df group by category").show()

print("-----2 col AGG SUM ----")

spark.sql("select sum(amount) as sum_amt,category,spendby from df group by category,spendby").show()

print("-----2 col AGG SUM with count ----")

spark.sql("select sum(amount) as sum_amt,category,spendby,count(*) as cnt from df group by category,spendby").show()

print("-----MAX amount of category ----")

spark.sql("select category,max(amount) as max_amt from df group by category").show()

print("-----MAX amount of category with order by ----")

spark.sql("select category,max(amount) as max_amt from df group by category order by category").show()

print("-----row_number ----")

spark.sql("select category,amount,"
          "row_number() OVER(partition by category order by amount) as row_num from df  ").show()

print("-----rank ----")

spark.sql("select category,amount,"
          "rank() OVER(partition by category order by amount) as row_num from df  ").show()

print("-----Dense rank ----")

spark.sql("select category,amount,"
          "dense_rank() OVER(partition by category order by amount) as row_num from df  ").show()



print("-----LEAD  ----")

spark.sql("select category,amount,"
          "lead(amount) OVER(partition by category order by amount desc) as lead from df  ").show()

print("-----LAG  ----")

spark.sql("select category,amount,"
          "lag(amount) OVER(partition by category order by amount desc) as lead from df  ").show()

print("-----HAVING  ----")

spark.sql("select category,count(category) as cnt from df group by category having count(category) >1").show()



# INNER JOIN


print("-----INNER JOIN  ----")

spark.sql("select a.*,b.product from cust a join prod b on a.id = b.id ").show()

print("-----LEFT JOIN  ----")

spark.sql("select a.*,b.product from cust a left join prod b on a.id = b.id ").show()

print("-----RIGHT JOIN  ----")

spark.sql("select a.*,b.product from cust a right join prod b on a.id = b.id ").show()

print("-----FULL JOIN  ----")

spark.sql("select a.*,b.product from cust a full join prod b on a.id = b.id ").show()

# LEFT ANTI JOIN


print("-----LEFT ANTI JOIN  ----")

spark.sql("select a.* from cust a left anti join prod b on a.id = b.id ").show()

###  youtube video link

##     https://www.youtube.com/watch?v=yVOTP0gy7TU


