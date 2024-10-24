# am_databricks_project

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
