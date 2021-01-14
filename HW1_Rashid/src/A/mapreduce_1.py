import mrjob
from mrjob.job import MRJob
import sys
import os
import logging
import datetime


logging.basicConfig(filename='mapreduce_1.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s',level=os.environ.get("LOGLEVEL", "INFO"))
logging.warning('This will get logged to a file')

REF_DATE = datetime.datetime(1995, 1, 1)

cust_dict = {"custkey":0,"name":1,"address":2, "nationkey":3, "phone":4,"acctbal":5,"mktsegment":6, "comment":7}
order_dict = {"orderkey":0, "custkey":1, "orderstatus":2, "price":3, "orderdate":4, 
        "orderpriority":5, "clerk":6, "shippriority":7, "comment":8}

class MRWordCount(MRJob):

    def mapper(self, _, line):
        line = line.strip()    # remove leading and trailing whitespace
        tokens = line.split("|")   # split the line into words

        custName = "-1"
        custAddress = "-1"
        orderPrice = "-1"
        acctbal = "-1"
        orderDate = "-1"


        if len(tokens) == 8: ## customer data
            acctbal = float(tokens[cust_dict["acctbal"]])
            if acctbal <= 1000:
                return
            custName = tokens[cust_dict["name"]]
            custAddress = tokens[cust_dict["address"]]
            custKey = tokens[cust_dict["custkey"]]
        else:
            orderDate = tokens[order_dict["orderdate"]]
            orderDate_dt = datetime.datetime.strptime(orderDate, '%Y-%m-%d')
            if orderDate_dt <= REF_DATE:
                return
            
            custKey = tokens[order_dict["custkey"]]
            orderPrice = tokens[order_dict["price"]]
        yield (custKey,(custName,custAddress,orderPrice))

    def reducer(self, key, values):

        currCust = None
        currAddress = None
        orderPrice = 0
        count = 0
       
        for item in values:
            custName,custAddress,price = item
            
            if custName != "-1":
                currCust = custName

            if custAddress != "-1":
                currAddress = custAddress

            if price != "-1":
                orderPrice += float(price)
                count += 1

        if currCust is not None and currAddress is not None and count != 0:
            avgPrice = orderPrice / count
            logging.info((key, (currCust, currAddress, avgPrice)))
            yield (key, (currCust, currAddress, avgPrice))

if __name__ == '__main__':
    MRWordCount.run()