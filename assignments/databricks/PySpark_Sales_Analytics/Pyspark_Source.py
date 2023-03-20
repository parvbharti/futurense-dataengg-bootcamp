# Databricks notebook source
from pyspark import SparkContext

# COMMAND ----------

rdd1=sc.textFile('/FileStore/tables/5000_Sales_Records.csv',2)

# COMMAND ----------

header =rdd1.first() 

# COMMAND ----------

rdd2 = rdd1.filter(lambda row:row != header) 

# COMMAND ----------

def frmt_dt(dt):
    updt=dt.split('/')[2]+dt.split('/')[0]+dt.split('/')[1]
    return int(updt)

# COMMAND ----------

rdd3=rdd2.map(lambda x:(x.split(',')[0],x.split(',')[1],x.split(',')[2],x.split(',')[3],x.split(',')[4],frmt_dt(x.split(',')[5]),x.split(',')[6],frmt_dt(x.split(',')[7]),int(x.split(',')[8]),float(x.split(',')[9]),float(x.split(',')[10]),float(x.split(',')[11]),float(x.split(',')[12]),float(x.split(',')[13])))

# COMMAND ----------

# 1. Display country wise number of orders
country_orders=rdd3.map(lambda x:(x[1],x[6])).groupBy(lambda x:x[0]).map(lambda x:(x[0],len(x[1])))

# COMMAND ----------

print(country_orders.take(15))

# COMMAND ----------

# 2.Display the number of units sold in each region.
regions_units=rdd3.map(lambda x:(x[0],x[8])).reduceByKey(lambda x,y:x+y)

# COMMAND ----------

print(regions_units.take(15))

# COMMAND ----------

# 3.Display the 10 most recent sales. 
from datetime import datetime


# COMMAND ----------

rd5= rdd3.map(lambda x : (x[6],datetime(int(x[5]))))

# COMMAND ----------

rd5 = rdd3.map(lambda row: row[:5] + [datetime.strptime(row[5],'%m/%d/%Y')] + row[6:])

# COMMAND ----------

rd5.take(2)

# COMMAND ----------

# 4.Display the products with atleast 2 occurences of 'a' (Using spark)
products_a_count=rdd3.map(lambda x:x[2]).filter(lambda x:x.count('a')>=2)

# COMMAND ----------

print(products_a_count.take(5))

# COMMAND ----------

# 5.Display country in each region with highest units sold. (Using spark)

# COMMAND ----------

country_sales = rdd3.map(lambda row : ((row[0] , row[1]),int(row[8])))
c_reduced = country_sales.reduceByKey(lambda a,b :a+b)


# COMMAND ----------

print(c_reduced.map(lambda x: (x[0][0],(x[0][1],x[1]))).reduceByKey(lambda a,b : max(a,b ,key=lambda x : x[1])).take(10))

# COMMAND ----------

# 6.Display the unit price and unit cost of each item in ascending order. (Using spark)
item_cost=rdd3.map(lambda x:(x[2],x[9],x[10])).distinct().sortBy(lambda x:x[2])

# COMMAND ----------

print(item_cost.take(20))

# COMMAND ----------

# 7.Display the number of sales yearwise. (Using pyspark)
yearwise_sales=rdd3.map(lambda x:(str(x[5])[:4],x[6])).groupBy(lambda x:x[0]).map(lambda x:(x[0],len(x[1])))

# COMMAND ----------

print(yearwise_sales.take(10))

# COMMAND ----------

# 8.Display the number of orders for each item. (Using pyspark)
item_orders=rdd3.map(lambda x:(x[2],x[6])).groupBy(lambda x:x[0]).map(lambda x:(x[0],len(x[1])))

# COMMAND ----------

print(item_orders.take(15))

# COMMAND ----------

# 9.Display the countr.with highest calallicing pyspark)

# COMMAND ----------


