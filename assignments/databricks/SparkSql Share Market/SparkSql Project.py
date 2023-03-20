# Databricks notebook source
from pyspark import SparkContext

# COMMAND ----------

shares_rdd = sc.textFile('/FileStore/tables/sharemarket.csv')


# COMMAND ----------

shares_rdd.take(5)

# COMMAND ----------

header = 'MARKET,SERIES,SYMBOL,SECURITY,PREV_CL_PR,OPEN_PRICE,HIGH_PRICE,LOW_PRICE,CLOSE_PRICE,NET_TRDVAL,NET_TRDQTY,CORP_IND,TRADES,HI_52_WK,LO_52_WK'

# COMMAND ----------

shares_rdd = shares_rdd.map(lambda x : x.split(','))

# COMMAND ----------

shares_rdd.take(5)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType



# COMMAND ----------

schema = StructType([ \
    StructField("MARKET",StringType(),True), \
    StructField("SERIES",StringType(),True), \
    StructField("SYMBOL",StringType(),True), \
    StructField("SECURITY", StringType(), True), \
    StructField("PREV_CL_PR", StringType(), True), \
    StructField("OPEN_PRICE", StringType(), True), \
    StructField("HIGH_PRICE", StringType(), True), \
    StructField("LOW_PRICE", StringType(), True), \
    StructField("CLOSE_PRICE", StringType(), True), \
    StructField("NET_TRDVAL", StringType(), True) ,\
    StructField("NET_TRDQTY", StringType(), True) ,\
    StructField("CORP_IND", StringType(), True), \
    StructField("TRADES", StringType(), True), \
    StructField("HI_52_WK", StringType(), True), \
    StructField("LO_52_WK", StringType(), True) \
  ])

# COMMAND ----------

shares_df = spark.createDataFrame(shares_rdd, schema)

# COMMAND ----------

shares_df.show(5)

# COMMAND ----------

shares_df.createOrReplaceTempView('ShareMarket')

# COMMAND ----------

spark.sql('desc ShareMarket').show()

# COMMAND ----------

spark.sql('Select * from ShareMarket limit 5').show()

# COMMAND ----------

# 1.Query to display the number of series present in the data.

result1 = spark.sql('Select count(distinct series) as Total_Count from ShareMarket')
result1.show()
result1.coalesce(1).write.format("csv").option("header", "true").mode("append").save("/FileStore/tables/output1.txt")

# COMMAND ----------

# 2.Display the series present in the data.(using hive)

result2 = spark.sql('Select Distinct(Series) from ShareMarket')
result2.show()
result2.coalesce(1).write.format("csv").option("header", "true").mode("append").save("/FileStore/tables/output2.txt")

# COMMAND ----------

# 3.Find the sumpof all the prices in the each series.(Using hive)

result3 = spark.sql('Select series, sum(prev_cl_pr), sum(open_price), sum(high_price), sum(low_price), sum(close_price) from ShareMarket GROUP BY series')
result3.show()
result3.coalesce(1).write.format("csv").option("header", "true").mode("append").save("/FileStore/tables/output3.txt")

# COMMAND ----------

# 4.Display security,series with highest net trade value(use pyspark)

result4 = spark.sql('Select series, security, net_trdval from ShareMarket where net_trdval = (Select Max(net_trdval) from ShareMarket)')
result4.show(truncate=False)
result4.coalesce(1).write.format("csv").option("header", "true").mode("append").save("/FileStore/tables/output4.txt")

# COMMAND ----------

# 5.Display the series whose sum of all prices greater than the net trade value.(Using pyspark)

result5 = spark.sql("select SERIES, round(PREV_CL_PR + OPEN_PRICE + HIGH_PRICE + LOW_PRICE + CLOSE_PRICE) as Total_price, NET_TRDVAL from ShareMarket where (PREV_CL_PR + OPEN_PRICE + HIGH_PRICE + LOW_PRICE + CLOSE_PRICE) > NET_TRDVAL ")
result5.show()
result5.coalesce(1).write.format("csv").option("header", "true").mode("append").save("/FileStore/tables/output5.txt")

# COMMAND ----------

# 6.Display the series with highest net trade quantity.(Using pyspark)
result6 = spark.sql('SELECT series, net_trdqty from ShareMarket Where net_trdqty = (Select max(net_trdqty) from ShareMarket)')
result6.show()
result6.coalesce(1).write.format("csv").option("header", "true").mode("append").save("/FileStore/tables/output6.txt")

# COMMAND ----------

# 7. Display the highest and lowest open price(Using sql)
result7 = spark.sql('Select max(open_price), min(open_price) from ShareMarket')
result7.show()
result7.coalesce(1).write.format("csv").option("header", "true").mode("append").save("/FileStore/tables/output7.txt")

# COMMAND ----------

# 8.Query to display the series which have trades more than 80.(Using SQL).
result8 = spark.sql('Select series,trades from ShareMarket where trades > 80')
result8.show()
result8.coalesce(1).write.format("csv").option("header", "true").mode("append").save("/FileStore/tables/output8.txt")

# COMMAND ----------

# 9.Display the difference between the net trade value net trade quantity for each series.(Using sql).
result9 = spark.sql('Select series, sum(round(net_trdval - net_trdqty)) as diff_net_trd_and_qty from ShareMarket Group By Series')
result9.show()
result9.coalesce(1).write.format("csv").option("header", "true").mode("append").save("/FileStore/tables/output9.txt")

# COMMAND ----------


