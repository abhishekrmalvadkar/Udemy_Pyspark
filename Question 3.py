# Databricks notebook source
# MAGIC %md
# MAGIC root
# MAGIC  |-- id: integer (nullable = false)
# MAGIC  |-- name: string (nullable = false)
# MAGIC  |-- date: string (nullable = false)
# MAGIC  |-- amount: integer (nullable = false)
# MAGIC  |-- category: string (nullable = false)
# MAGIC  Write a PySpark code to find the total amount spent in each category for the year 2022. Display the results in descending order of the total amount spent.

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
data = ([1001,"Abhishek","2023-03-01",1000,"hardware"],
       [1002,"asd","2023-03-02",1000,"software"],
       [1003,"fdf","2023-03-03",1000,"software"],
       [1004,"rw","2023-03-04",1000,"hardware"],
       [1005,"tyhtht","2021-03-05",1000,"hardware"],
       [1006,"gnvvn","2021-03-06",1000,"hardware"],
       [1007,"qweqqwe","2022-03-07",1000,"hardware"])

headers = StructType([StructField("id",IntegerType(),False),
                    StructField("name",StringType(),False),
                    StructField("date",StringType(),False),
                      StructField("amount",IntegerType(),False),
                      StructField("category",StringType(),False)])

df = spark.createDataFrame(data, schema=headers)

display(df)

# COMMAND ----------

total_spent = df.filter(year("date") == 2023).groupBy("category") \
                .agg(sum("amount").alias("total_amount_spent")) \
                .orderBy(desc("total_amount_spent"))
total_spent.display()

# COMMAND ----------


