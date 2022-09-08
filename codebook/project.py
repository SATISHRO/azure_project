# Databricks notebook source
# DBTITLE 1,mount_point
dbutils.fs.mount(
  source = "wasbs://sales@satish22.blob.core.windows.net",
  mount_point = "/mnt/satish22",
  extra_configs = {"fs.azure.account.key.satish22.blob.core.windows.net":"eMvY0GFTsqVmmU/XTndcS3xdH5B8Rx97l6Y3NtQWgkZcIs1ushyAl9OZVNFfHbBnCUQEDZ6dY22C+AStxkRWfg=="})

# COMMAND ----------

# DBTITLE 1,Read data
April = spark.read.csv("/mnt/satish22/sales2019/Sales_April_2019.csv",header=True)

August = spark.read.csv("/mnt/satish22/sales2019/Sales_August_2019.csv",header=True)

Decembe = spark.read.csv("/mnt/satish22/sales2019/Sales_February_2019.csv",header=True)

February = spark.read.csv("/mnt/satish22/sales2019/Sales_April_2019.csv",header=True)

January = spark.read.csv("/mnt/satish22/sales2019/Sales_January_2019.csv",header=True)

July = spark.read.csv("/mnt/satish22/sales2019/Sales_July_2019.csv",header=True)

June = spark.read.csv("/mnt/satish22/sales2019/Sales_June_2019.csv",header=True)

March = spark.read.csv("/mnt/satish22/sales2019/Sales_March_2019.csv",header=True)

May = spark.read.csv("/mnt/satish22/sales2019/Sales_May_2019.csv",header=True)

November = spark.read.csv("/mnt/satish22/sales2019/Sales_November_2019.csv",header=True)

October = spark.read.csv("/mnt/satish22/sales2019/Sales_October_2019.csv",header=True)

September = spark.read.csv("/mnt/satish22/sales2019/Sales_September_2019.csv",header=True)

# COMMAND ----------

# DBTITLE 1,Data cleaning 
April.printSchema()

# COMMAND ----------

# DBTITLE 1,Update Column names
April = April.withColumnRenamed("Order ID","Order_ID").withColumnRenamed("Price Each","Price_Each").withColumnRenamed("Purchase Address","Purchase_Address").withColumnRenamed("Order Date","Order_Date").withColumnRenamed("Quantity Ordered","Quantity_Ordered")

August = August.withColumnRenamed("Order ID","Order_ID").withColumnRenamed("Price Each","Price_Each").withColumnRenamed("Purchase Address","Purchase_Address").withColumnRenamed("Order Date","Order_Date").withColumnRenamed("Quantity Ordered","Quantity_Ordered")

Decembe = Decembe.withColumnRenamed("Order ID","Order_ID").withColumnRenamed("Price Each","Price_Each").withColumnRenamed("Purchase Address","Purchase_Address").withColumnRenamed("Order Date","Order_Date").withColumnRenamed("Quantity Ordered","Quantity_Ordered")

February = February.withColumnRenamed("Order ID","Order_ID").withColumnRenamed("Price Each","Price_Each").withColumnRenamed("Purchase Address","Purchase_Address").withColumnRenamed("Order Date","Order_Date").withColumnRenamed("Quantity Ordered","Quantity_Ordered")

January = January.withColumnRenamed("Order ID","Order_ID").withColumnRenamed("Price Each","Price_Each").withColumnRenamed("Purchase Address","Purchase_Address").withColumnRenamed("Order Date","Order_Date").withColumnRenamed("Quantity Ordered","Quantity_Ordered")

July = July.withColumnRenamed("Order ID","Order_ID").withColumnRenamed("Price Each","Price_Each").withColumnRenamed("Purchase Address","Purchase_Address").withColumnRenamed("Order Date","Order_Date").withColumnRenamed("Quantity Ordered","Quantity_Ordered")

June = June.withColumnRenamed("Order ID","Order_ID").withColumnRenamed("Price Each","Price_Each").withColumnRenamed("Purchase Address","Purchase_Address").withColumnRenamed("Order Date","Order_Date").withColumnRenamed("Quantity Ordered","Quantity_Ordered")

March = March.withColumnRenamed("Order ID","Order_ID").withColumnRenamed("Price Each","Price_Each").withColumnRenamed("Purchase Address","Purchase_Address").withColumnRenamed("Order Date","Order_Date").withColumnRenamed("Quantity Ordered","Quantity_Ordered")

May = May.withColumnRenamed("Order ID","Order_ID").withColumnRenamed("Price Each","Price_Each").withColumnRenamed("Purchase Address","Purchase_Address").withColumnRenamed("Order Date","Order_Date").withColumnRenamed("Quantity Ordered","Quantity_Ordered")

November = November.withColumnRenamed("Order ID","Order_ID").withColumnRenamed("Price Each","Price_Each").withColumnRenamed("Purchase Address","Purchase_Address").withColumnRenamed("Order Date","Order_Date").withColumnRenamed("Quantity Ordered","Quantity_Ordered")

October = October.withColumnRenamed("Order ID","Order_ID").withColumnRenamed("Price Each","Price_Each").withColumnRenamed("Purchase Address","Purchase_Address").withColumnRenamed("Order Date","Order_Date").withColumnRenamed("Quantity Ordered","Quantity_Ordered")

September = September.withColumnRenamed("Order ID","Order_ID").withColumnRenamed("Price Each","Price_Each").withColumnRenamed("Purchase Address","Purchase_Address").withColumnRenamed("Order Date","Order_Date").withColumnRenamed("Quantity Ordered","Quantity_Ordered")

# COMMAND ----------

April.printSchema()

# COMMAND ----------

# DBTITLE 1,Data Modification
from pyspark.sql.types import DoubleType, IntegerType ,FloatType 

August = August.withColumn("Order_ID", August["Order_ID"].cast(IntegerType())).withColumn("Quantity_Ordered", August["Quantity_Ordered"].cast(IntegerType())).withColumn("Price_Each", August["Price_Each"].cast(FloatType()))

April = April.withColumn("Order_ID", April["Order_ID"].cast(IntegerType())).withColumn("Quantity_Ordered", April["Quantity_Ordered"].cast(IntegerType())).withColumn("Price_Each", April["Price_Each"].cast(FloatType()))

Decembe = Decembe.withColumn("Order_ID", Decembe["Order_ID"].cast(IntegerType())).withColumn("Quantity_Ordered", Decembe["Quantity_Ordered"].cast(IntegerType())).withColumn("Price_Each", Decembe["Price_Each"].cast(FloatType()))

February = February.withColumn("Order_ID", February["Order_ID"].cast(IntegerType())).withColumn("Quantity_Ordered", February["Quantity_Ordered"].cast(IntegerType())).withColumn("Price_Each", February["Price_Each"].cast(FloatType()))

January = January.withColumn("Order_ID", January["Order_ID"].cast(IntegerType())).withColumn("Quantity_Ordered", January["Quantity_Ordered"].cast(IntegerType())).withColumn("Price_Each", January["Price_Each"].cast(FloatType()))

July = July.withColumn("Order_ID", July["Order_ID"].cast(IntegerType())).withColumn("Quantity_Ordered", July["Quantity_Ordered"].cast(IntegerType())).withColumn("Price_Each", July["Price_Each"].cast(FloatType()))

June = June.withColumn("Order_ID", June["Order_ID"].cast(IntegerType())).withColumn("Quantity_Ordered", June["Quantity_Ordered"].cast(IntegerType())).withColumn("Price_Each", June["Price_Each"].cast(FloatType()))

March = March.withColumn("Order_ID", March["Order_ID"].cast(IntegerType())).withColumn("Quantity_Ordered", March["Quantity_Ordered"].cast(IntegerType())).withColumn("Price_Each", March["Price_Each"].cast(FloatType()))

May = May.withColumn("Order_ID", May["Order_ID"].cast(IntegerType())).withColumn("Quantity_Ordered", May["Quantity_Ordered"].cast(IntegerType())).withColumn("Price_Each", May["Price_Each"].cast(FloatType()))

November = November.withColumn("Order_ID", November["Order_ID"].cast(IntegerType())).withColumn("Quantity_Ordered", November["Quantity_Ordered"].cast(IntegerType())).withColumn("Price_Each", November["Price_Each"].cast(FloatType()))

October = October.withColumn("Order_ID", October["Order_ID"].cast(IntegerType())).withColumn("Quantity_Ordered", October["Quantity_Ordered"].cast(IntegerType())).withColumn("Price_Each", October["Price_Each"].cast(FloatType()))

September = September.withColumn("Order_ID", September["Order_ID"].cast(IntegerType())).withColumn("Quantity_Ordered", September["Quantity_Ordered"].cast(IntegerType())).withColumn("Price_Each", September["Price_Each"].cast(FloatType()))


# COMMAND ----------

August1.printSchema()

# COMMAND ----------

shape=(April.count(),len(April.columns))
print(shape)

# COMMAND ----------

April = April.na.drop()
August = April.na.drop()
Decembe = April.na.drop()
February = April.na.drop()
January = April.na.drop()
July = April.na.drop()
June = April.na.drop()
March = April.na.drop()
May = April.na.drop()
November = April.na.drop()
October = April.na.drop()
September = April.na.drop()

# COMMAND ----------

shape=(April.count(),len(April.columns))
print(shape)

# COMMAND ----------

# Check total Values 
import pandas as pd
April1 =April.toPandas()
April1.isnull().sum().sort_values(ascending=False)

# COMMAND ----------

April.createOrReplaceTempView("April")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select Order_ID, sum(Quantity_Ordered*Price_Each) as total_PAY from April group by Order_ID;
# MAGIC -- SELECT * FROM APRIL
# MAGIC WITH ctv as(
# MAGIC select Product ,
# MAGIC sum(Quantity_Ordered*Price_Each)  as total_PAY 
# MAGIC from April group by Product)
# MAGIC select * from ctv order by total_PAY desc limit 10;

# COMMAND ----------

# DBTITLE 1,End

