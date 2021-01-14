#!/usr/bin/env python

from operator import itemgetter
import sys

currentKey = None
currentName = "-1"
currentAddress = "-1"
total_price = 0
count = 0

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    print(line)

    # parse the input we got from mapper.py
    custKey,custName,custAddress,orderPrice = line.split('|')

    if currentKey is None:
        currentKey = custKey
        total_price += float(orderPrice)
        count += 1
    elif currentKey == custKey:
        total_price += float(orderPrice)
        count += 1
    else:
        print("Customer Key: ", currentKey)
        print("Customer Name:", currentName)
        avg_price = total_price / (count - 1)
        print(total_price, count)

        if currentName != "-1":
            print(currentKey, currentName, currentAddress, avg_price)

        total_price = float(orderPrice)
        count = 1
        currentKey = custKey
        currentName = custName
        currentAddress = custAddress

    if custName != "-1":
        currentName = custName

    if custAddress != "-1":
        currentAddress = custAddress

        
         
#         #the first line should be a mapping line, otherwise we need to set the currentCountryName to not known
#         if personName == "-1": #this is a new country which may or may not have people in it
#             currentCountryName = countryName
#             currentCountry2digit = country2digit
#             isCountryMappingLine = True
#         else:
#             isCountryMappingLine = False # this is a person we want to count
         
#         if not isCountryMappingLine: #we only want to count people but use the country line to get the right name 
 
#             #first check to see if the 2digit country info matches up, might be unkown country
#             if currentCountry2digit != country2digit:
#                 currentCountry2digit = country2digit
#                 currentCountryName = '%s - Unkown Country' % currentCountry2digit
             
#             currentKey = '%s\t%s' % (currentCountryName,personType) 
             
#             if foundKey != currentKey: #new combo of keys to count
#                 if isFirst == 0:
#                     print('%s\t%s' % (foundKey,currentCount))
#                     currentCount = 0 #reset the count
#                 else:
#                     isFirst = 0
             
#                 foundKey = currentKey #make the found key what we see so when we loop again can see if we increment or print out
             
#             currentCount += 1 # we increment anything not in the map list
#     except:
#         pass
 
# try:
#     print('%s\t%s' % (foundKey,currentCount))
# except:
#     pass
