# Databricks notebook source
import dlt
import pyspark.sql.functions as F

# COMMAND ----------

access_key = dbutils.secrets.get(scope="s3_secrets", key="access_key")
secret_key = dbutils.secrets.get(scope="s3_secrets", key="secret_key")
spark.conf.set("fs.s3a.access.key", access_key)
spark.conf.set("fs.s3a.secret.key", secret_key)
spark.conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
file_location = "s3a://sales-files-akash/sales_data.csv"
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("quote", '"') \
    .option("escape", '"') \
    .option("multiline", "true") \
    .option("inferSchema", "true") \
    .load(file_location)
df.show(2)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

#Requirement Analysis - GOLD
# Sales data of Customers(Customer_Age,Age_Group) -- dim_customer_group
# Products(Product_Category,Sub_Category,Product) -- dim_products
# Location(State, Country, Country_code)                        -- dim_location
# Customer_Gender - low cardinality dimention     -- dim_gender
# Date_dim(DAY,MONTH,YEAR,DATE)                   -- dim_date

# (index,date,customer_group_id,product_id,gender,Order_Quantity,Unit_Cost,Unit_Price,Profit,Cost,Revenue)   -- fact_sales

# Business Views -  GOLD
# OBT using dim and facts
# Profit PER - State, country
# Revenue based on gender overall

# Data Analysis - Silver
# Check for null values, replace with average value in the column
# Country_code logic: if one word: First 3 letters in caps, if two...first letters in caps.
# Complicated - (Optional, next iteration) : Age_Group into Min_Age, Max_Age, Age_Group_Name

# Bronze - Date format in: DD/MM/YYYY instead of YYYY-MM-DD

# COMMAND ----------

# Date Format Change Logic
from pyspark.sql.functions import date_format,avg

display(df.withColumn('Date', date_format(df['Date'], "dd/MM/yyyy")))

# COMMAND ----------

# Data Analysis - Silver
# Check for null values, replace with average value in the column
# Country_code logic: if one word: First 3 letters in caps, if two...first letters in caps.

df.where(df['Order_Quantity'].isNull()).count()

# COMMAND ----------

#to find average value
df.select(avg(df['Order_Quantity'])).collect()[0][0]

# COMMAND ----------

## for all cols
datatype_dict = {}
for col_name, col_type in df.dtypes:
    datatype_dict[col_name] = col_type

for col in df.columns:
    count = df.where(df[col].isNull()).count()
    if count != 0:
        if datatype_dict[col] != 'integer':
            #drop null rows
            df.na.drop(how ='any')
        else:
            average = df.select(avg(df[col])).collect()[0][0]

# COMMAND ----------

# Country_code logic: if one word: First 3 letters in caps, if two...first letters in caps.
#Using UDF here

def get_country_code(country):
    if len(country.split()) == 1:
        return country[0:2].upper()
    else:
        code = ''
        for word in country.split():
            code=code+str(word[0])
        return code.upper()
udf_f = udf(get_country_code)
    
display(df.withColumn('Country_Code',udf_f(df['Country'])))   

# COMMAND ----------

a= 'Australia'
a.split()

# COMMAND ----------

len(a.split())
