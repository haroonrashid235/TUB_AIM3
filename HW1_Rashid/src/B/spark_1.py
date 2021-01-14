from pyspark import SparkContext
from pyspark.sql import SparkSession
import pandas as pd
import os

import datetime


output_dir = 'output' 
os.makedirs(output_dir, exist_ok=True)

orders_file = '../data/orders.csv'
customer_file = '../data/customers.csv'

REF_DATE = datetime.datetime(1995, 1, 1)



spark = SparkSession.builder \
    .master("local") \
    .appName("Spark1") \
    .getOrCreate()

if __name__ == "__main__":
    # create Spark context with necessary configuration
    # sc = SparkContext()
    orders_df = spark.read.format('com.databricks.spark.csv')\
                .options(header='false',delimiter='|')\
                .load(orders_file)\
                .toDF("orderkey","custkey","orderstatus","price","orderdate","orderpriority","clerk","shippriority","comment")


    customers_df = spark.read.format('com.databricks.spark.csv')\
                .options(header='false',delimiter='|')\
                .load(customer_file)\
                .toDF("custkey","name","addres","nationkey","phone","acctbal","mktsegment","comment")

    customers_df = customers_df.filter(customers_df.acctbal > 1000).select("custkey","name","addres")

    orders_df = orders_df.filter(orders_df["orderdate"] > (REF_DATE)).groupBy("custkey")\
                .agg({"price":"avg"})

    joined_df = customers_df.join(orders_df, customers_df.custkey == orders_df.custkey).drop('custkey')
    joined_df.show()

    ## Repartioning to save dataframe in 1 partition for readibility
    joined_df.repartition(1).write.csv(os.path.join(output_dir, 'spark1'))
    print(f"Head: {joined_df.take(5)}")