# Databricks notebook source
Assume you have a PySpark DataFrame df with the following schema:

objectivec
Copy code
root
 |-- id: integer (nullable = false)
 |-- name: string (nullable = false)
 |-- salary: integer (nullable = false)
 |-- department: string (nullable = false)
Write a PySpark code to find the top 5 departments with the highest average salary. Display the results in descending order of the average salary.

Good luck!


# COMMAND ----------

from pyspark.sql.functions import *
data = ([1001,"Abhishek",1000,"CS"],
       [1002,"sakshi",2000,"CS"],
       [1003,"karan",3000,"EC"],
       [1004,"ravi",4000,"EC"],
       [1005,"jyoti",5000,"IS"],
       [1006,"aryan",6000,"IS"],
       [1007,"soumya",7000,"ME"],
       [1008,"malvadkar",8000,"ME"])

headers = (["id","name","salary","department"])

df = spark.createDataFrame(data,schema=headers)
display(df)

# COMMAND ----------

avg_salary_df = df.groupBy("department").agg(avg("salary").alias("Average_Salary")).orderBy(desc("Average_Salary")).limit(2)
avg_salary_df.display()
