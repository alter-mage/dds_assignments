#
# Assignment5 Interface
# Name: 
#

from pymongo import MongoClient
import os
import sys
import json
import math

def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
    # pass
    save_location = open(saveLocation1, 'a+')
    
    for document in collection.find({}):
        if document['city'].upper() == cityToSearch.upper() and document['type'] == 'business':
            output_string_builder = '%s$%s$%s$%s\n' % (document['name'].upper(), document['full_address'].upper(), document['city'].upper(), document['state'].upper())
            save_location.write(output_string_builder)
    save_location.close()


def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    # pass
    save_location = open(saveLocation2, 'a+')
    
    for document in collection.find({}):
        business_condition = document['type'] == 'business'
        category_condition = False
        for category in categoriesToSearch:
            category_condition = category_condition or category in document['categories']
        r = 3959
        fi_1 = math.radians(document['latitude'])
        fi_2 = math.radians(float(myLocation[0]))
        delta_fi = math.radians(float(myLocation[0]) - document['latitude'])
        delta_lambda = math.radians(float(myLocation[1]) - document['longitude'])
        a = (math.sin(delta_fi/2)**2) + math.cos(fi_1)*math.cos(fi_2)*(math.sin(delta_lambda/2)**2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        d = r * c
        distance_condition = d <= maxDistance
        if business_condition and category_condition and distance_condition:
            save_location.write('%s\n' % (document['name'].upper()))
    save_location.close()