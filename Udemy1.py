# Databricks notebook source
# MAGIC %md
# MAGIC root
# MAGIC  |-- address_id: long (nullable = true)
# MAGIC  |-- birth_country: string (nullable = true)
# MAGIC  |-- birthdate: string (nullable = true)
# MAGIC  |-- customer_id: long (nullable = true)
# MAGIC  |-- demographics: struct (nullable = true)
# MAGIC  |    |-- buy_potential: string (nullable = true)
# MAGIC  |    |-- credit_rating: string (nullable = true)
# MAGIC  |    |-- education_status: string (nullable = true)
# MAGIC  |    |-- income_range: array (nullable = true)
# MAGIC  |    |    |-- element: long (containsNull = true)
# MAGIC  |    |-- purchase_estimate: long (nullable = true)
# MAGIC  |    |-- vehicle_count: long (nullable = true)
# MAGIC  |-- email_address: string (nullable = true)
# MAGIC  |-- firstname: string (nullable = true)
# MAGIC  |-- gender: string (nullable = true)
# MAGIC  |-- is_preffered_customer: string (nullable = true)
# MAGIC  |-- lastname: string (nullable = true)
# MAGIC  |-- salutation: string (nullable = true)

# COMMAND ----------

# DBTITLE 1,Creating and defining a dataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import col
df_schema =  StructType ([
    StructField("address_id",IntegerType(),True),
    StructField("birth_country",StringType(),True),
    StructField("birthdate",DateType(),True),
    StructField("customer_id",IntegerType(),True),
    StructField("demographics",
               StructType([
                   StructField("buy_potential",StringType(),True),
                   StructField("credit_rating",StringType(),True),
                   StructField("education_status",StringType(),True),
                   StructField("income_range",ArrayType(IntegerType(),True),True),
                   StructField("purchase_estimate",IntegerType(),True),
                   StructField("vehicle_count",IntegerType(),True)
               ])),
    StructField("email_address",StringType(),True),
    StructField("firstname",StringType(),True),
    StructField("gender",StringType(),True),
    StructField("is_preffered_customer",StringType(),True),
    StructField("lastname",StringType(),True),
    StructField("salutation",StringType(),True)
    
])

df = spark.read.format("json").schema(df_schema).load("dbfs:/FileStore/dcad_data/customer.json")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# DBTITLE 1,Selecting columns
from pyspark.sql.functions import col,expr,year
df.select("lastname","demographics.buy_potential",col("firstname"),expr("concat(firstname,' ',lastname) fullname")).show(n=3)
df.select(year("birthdate")).show(n=3)

# COMMAND ----------

# DBTITLE 1,Renaming columns
df.withColumnRenamed("email_address","email").printSchema()


# COMMAND ----------

# DBTITLE 1,Changing data types
import pyspark.sql.functions
df=df.withColumn("address_id",df.address_id.cast("string"))
df.printSchema()

# selectExpr("cast(customer_id as string)"): THIS WILL ONLY RETURN ONE COLUMN SCHEMA

# COMMAND ----------

# DBTITLE 1,Adding columns 
from pyspark.sql.functions import lit,concat
df=df.withColumn("Company_name",lit("DiggiBytes")).withColumn("Company_address",lit("Bengaluru")).withColumn("fullName",concat("firstname","lastname")).withColumn("Address&Fullname", expr("address_id + 1"))
df.printSchema()
df.display()

# COMMAND ----------

# DBTITLE 1,Drop columns 
df.drop("Address&Fullname").printSchema()

# COMMAND ----------

# DBTITLE 1,Filtering a DF
df.filter(year("birthdate")>1900).where("day(birthdate)>15").show(n=3)
#filter(month('birthdate')===1).show(n=3)

# COMMAND ----------

# DBTITLE 1,Drop Duplicate rows
# MAGIC %md
# MAGIC data_dup = [(1,"Abhishek"),(1,"Abhishek"),(2,"ek"),(3,"Abhishek")]
# MAGIC schema_dup = ["id","name"]
# MAGIC 
# MAGIC df_duplicate=spark.createDataFrame(data_dup,schema_dup)
# MAGIC df_duplicate.show()
# MAGIC df_duplicate.distinct().show()
# MAGIC df_duplicate.dropDuplicates(["id"]).show()

# COMMAND ----------

# DBTITLE 1,Handling Null
display(df.where(col("salutation").isNull()))
df.where(col("salutation").isNull()).count

# COMMAND ----------

# DBTITLE 1,Drop null values
#df.na.drop(how="all").display()
#df.na.drop(how="any").display()

df.na.drop(subset=["firstname","lastname"]).display()
#df.where("salutation is NULL").display()
df.na.fill({"salutation":"unknown"}).display()

df.where("salutation is unknown").display()

# COMMAND ----------

# DBTITLE 1,Sort and OrderBy
df.na.drop("any").sort("firstname").orderBy(col("birthdate").desc()).select("firstname","lastname",'birthdate').display()

# COMMAND ----------

# DBTITLE 1,GroupBy
#df.groupBy("birthdate").count().show()

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,sum,avg,max

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

simpleData = [("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  ]

schema = ["employee_name","department","state","salary","age","bonus"]
df = spark.createDataFrame(data=simpleData, schema = schema)
df.printSchema()
df.show(truncate=False)

df.groupBy("department").sum("salary").show(truncate=False)

df.groupBy("department").count().show(truncate=False)


df.groupBy("department","state") \
    .sum("salary","bonus") \
   .show(truncate=False)

df.groupBy("department") \
    .agg(sum("salary").alias("sum_salary"), \
         avg("salary").alias("avg_salary"), \
         sum("bonus").alias("sum_bonus"), \
         max("bonus").alias("max_bonus") \
     ) \
    .show(truncate=False)
    
df.groupBy("department") \
    .agg(sum("salary").alias("sum_salary"), \
      avg("salary").alias("avg_salary"), \
      sum("bonus").alias("sum_bonus"), \
      max("bonus").alias("max_bonus")) \
    .where(col("sum_bonus") >= 50000) \
    .show(truncate=False)




# COMMAND ----------

# DBTITLE 1,Aggregate functions
df.groupBy("department") \
    .agg(sum("salary").alias("sum_salary"), \
      avg("salary").alias("avg_salary"), \
      sum("bonus").alias("sum_bonus"), \
      max("bonus").alias("max_bonus")) \
    .where(col("sum_bonus") >= 50000) \
    .show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Joins 
from pyspark.sql.functions import col,expr,year

simpleData1 = [("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  ]

schema1 = ["employee_name","department","state","salary","age","bonus"]
df1 = spark.createDataFrame(data=simpleData1, schema = schema1)

df1.display()



simpleData2 = [("A","Sales","NY",90000,34,10000),
    ("b","Sales","NY",86000,56,20000),
    ("c","Sales","CA",81000,30,23000),
    ("d","Finance","CA",90000,24,23000),
    ("e","Finance","CA",99000,40,24000),
    ("f","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  ]

schema2 = ["employee_name","department","state","salary","age","bonus"]
df2 = spark.createDataFrame(data=simpleData2, schema = schema2)

df2.display()

df1.join(df2, df1.employee_name == df2.employee_name,"inner").show()
df1.join(df2, df1.employee_name == df2.employee_name,"outer").show()
df1.join(df2, df1.employee_name == df2.employee_name,"cross").show()
df1.join(df2, df1.employee_name == df2.employee_name,"left").show()
df1.join(df2, df1.employee_name == df2.employee_name,"right").show()


df1.union(df2).distinct().show()
df1.union(df2).where(df1.employee_name == "James").show()

# COMMAND ----------

# DBTITLE 1,Saving data with PartitionBy 
df.write.partitionBy("department").option("path","/dbfs/tmp/Udemy_Save1").mode('overwrite').save()

#df.write.format("csv").mode('overwrite').save("/dbfs/tmp/Udemy_Save")

# COMMAND ----------

# MAGIC %fs ls /dbfs/tmp/Udemy_Save1

# COMMAND ----------

# DBTITLE 1,UDF
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

UpperCaseUdf = udf(lambda a: str(a).upper(), StringType())
df.withColumn("department_UpperCase", UpperCaseUdf(col("department"))).show()


# COMMAND ----------


