import mrjob
from mrjob.job import MRJob
import sys
import os
import logging
import datetime


output_dir = 'output' 
os.makedirs(output_dir, exist_ok=True)

logging.basicConfig(filename=os.path.join(output_dir, 'mapreduce_2'), filemode='w', format='%(name)s - %(levelname)s - %(message)s',level=os.environ.get("LOGLEVEL", "INFO"))

REF_DATE = datetime.datetime(1995, 1, 1)

cust_dict = {"custkey":0,"name":1,"address":2, "nationkey":3, "phone":4,"acctbal":5,"mktsegment":6, "comment":7}
order_dict = {"orderkey":0, "custkey":1, "orderstatus":2, "price":3, "orderdate":4, 
        "orderpriority":5, "clerk":6, "shippriority":7, "comment":8}

class MRWordCount(MRJob):

    def mapper(self, key, line):
        line = line.strip()    # remove leading and trailing whitespace
        tokens = line.split("|")   # split the line into words

        if len(tokens) == 8: ## customer data
            custKey = tokens[cust_dict["custkey"]]
            custName = tokens[cust_dict["name"]]
            yield (custKey, custName)
        else:
            custKey = tokens[order_dict["custkey"]]
            yield (custKey, 1)

    def reducer(self, key, values):
        values = list(values)
        if len(values) == 1:
            yield (key, values[0])

if __name__ == '__main__':
    MRWordCount.run()