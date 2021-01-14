#!/usr/bin/env python

import sys
import datetime

cust_dict = {"custkey":0,"name":1,"address":2, "nationkey":3, "phone":4,"acctbal":5,"mktsegment":6, "comment":7}
order_dict = {"orderkey":0, "custkey":1, "orderstatus":2, "price":3, "orderdate":4, 
        "orderpriority":5, "clerk":6, "shippriority":7, "comment":8}

REF_DATE = datetime.datetime(1995, 1, 1)

for line in sys.stdin:    # Input is read from STDIN and the output of this file is written into STDOUT
    line = line.strip()    # remove leading and trailing whitespace
    tokens = line.split("|")   # split the line into words

    custName = "-1"
    custAddress = "-1"
    orderPrice = "-1"


    if len(tokens) == 8: ## customer data
        # print("customer record..")
        acctbal = float(tokens[cust_dict["acctbal"]])
        if acctbal <= 1000:
            continue
        custName = tokens[cust_dict["name"]]
        custAddress = tokens[cust_dict["address"]]
        custKey = tokens[cust_dict["custkey"]]
    else:
        # print("Order record")
        orderDate = tokens[order_dict["orderdate"]]
        orderDate_dt = datetime.datetime.strptime(orderDate, '%Y-%m-%d')
        if orderDate_dt <= REF_DATE:
            continue
        # print("OrderDate: ", orderDate_dt.date())
        custKey = tokens[order_dict["custkey"]]
        orderPrice = tokens[order_dict["price"]]

    print('%s|%s|%s|%s' % (custKey,custName,custAddress,orderPrice))

    
    
    # for word in words:   
    #     print(word + '\t' + str(1))   #Print all words (key) individually with the value 1