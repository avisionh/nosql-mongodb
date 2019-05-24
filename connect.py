# - MongoDB x Python - #
# Description: This script demonstrates the following:
#   1. Connecting to MongoDB Atlas
#   2. Writing data to a database 
#   3. Reading data from a database
#   4. Analysing data from a database

# Note: The password for READ-WRITE access to this database is hidden in this script.
#       Please request the password from a_vision@hotmail.co.uk  

# Reference: https://docs.mongodb.com/guides/server/read/

from pymongo import MongoClient
from pprint import pprint
import kaggle
import json
import os

# check current working directory
os.getcwd()

# authenticate Kaggle API to download from there
kaggle.api.authenticate()

# 1. Connect to Mongo instance
# desc: put connection code in a class so that it can be reused.
class Connect(object):
    @staticmethod    
    def get_connection():
        return MongoClient("mongodb://user_readwrite:<pw>@cluster-open-shard-00-00-kzzlc.mongodb.net:27017,cluster-open-shard-00-01-kzzlc.mongodb.net:27017,cluster-open-shard-00-02-kzzlc.mongodb.net:27017/test?replicaSet=cluster-open-shard-0&authSource=admin&ssl=true")

# 2. Call the class just created.
client = Connect.get_connection()

# 3. Access the 'library' database
db = client.library

# 4. Retrieve all documents in 'authors' collection within 'library database
cursor = db.authors.find({})
for authors in cursor:
     pprint(authors)

     
# - Data Import - #
# 1. Retrive data from Kaggle API and put in relevant folder
path_folder = "C:/Users/a_vis/Documents/Data Science/Python/MongoDB/data"
kaggle.api.dataset_download_files('adithyarganesh/english-premier-league-player-data-20182019', 
                                  path = path_folder, 
                                  unzip = True)

# 2. Import EPL data into Python
with open(path_folder + '/fpl_data_2018_2019.json') as data_football:
    file_data = json.load(data_football)

# convert to list for importing as multiple documents
# this does't quite work since we don't have key-value pairs
# for players e.g. player:"Rolando Aarons"
# file_data_list = file_data.items()
    
# 3. 'Create' database and collection for new data
#   note: not actually creating these in mongoDB, but in Python are
db = client.footballDB
collection_england = db.england
# 3. Import EPL data into Mongo
collection_england.insert_one(file_data)

# 4. Check it has been imported
cursor = db.england.find({})
for england in cursor:
     pprint(england)
     
# 5. Drop collection in case import went wrong
db.england.drop()

# - Data Import: Secondary - #
# 1. Import Country data for constructing links between country data
kaggle.api.dataset_download_files('timoboz/country-data',
                                  path = path_folder,
                                  unzip = True)

# 2. Import country json file into Python session
data_dicts = []
for file in os.listdir(path_folder):
    full_filename = "%s/%s" % (path_folder, file)
    with open(full_filename,'r') as fi:
        dict = json.load(fi)
        data_dicts.append(dict)
        
del dict; del file; del full_filename


# 3. Remove 4th and 5th list items since they are already in database
# note: use pop() instead of del() because want to return the dict item
#        being removed and store in a list
# note: perform same operation 2 times so want to do a loop
#        but don't want to store iterator so use '_' instead
# reference: https://stackoverflow.com/a/2970808
data_remove = [] 
for _ in range(2):
    data_remove.append(data_dicts.pop(3))

# 4. Create database and collections for new data
# note: these are not created in mongoDB yet,
#        only exist in Python.
#       when import into mongoDB, only then do we create
#        the database and collections.
db = client['countryDB']

# follow collections naming-convention here:
#   https://stackoverflow.com/questions/5916080/what-are-naming-conventions-for-mongodb

# idea here is to use a loop to iterate the MongoDB import for each collection:
    # db.<name>.insert_many(data_dicts[i])
# as have different variable names,
# need object which can be called from to use as variable names. e.g. collection_<name>
# is possible by using dictionary keys; not possible with lists

# create two lists so can create a dictionary,
# which is needed to use elements as variable names
# so can loop over for each dictionary in data_dicts
# to import into mongoDB
collections = ['capitals','continents','currencies','isothree','names','phonecodes']
# No need for this anymore since using db[key] instead!
# redundant: db_collections = ['db.' + element for element in collections]

dict_collections = {k:v for k, v in zip(collections, data_dicts)}
# 3. Import data into Mongo
# Principle for how this nested loop works is
#   i. initialise outer loop
#   ii. for each key-value pair in dict_collections, e.g. 'capitals': {'AD':'Andorra la Vella', 'AE':'Abu Dhabi',...}
#   iii. create an empty list object, list_dict,
#        so we can append sub-key:sub-value objects in (wrapping them with '{}') e.g. [{'AD':'Andorra la Vella'},{'AE':'Abu Dhabi'},...].
#       purpose of storing as list object is so can use `insert_many` function
#        to import more than one document into mongoDB
#   iv. intialise sub-loop
#   v. for each (sub-)key-(sub-)value pair in the 'value' field of dict_collections, e.g. {'AD':'Andorra la Vella', 'AE':'Abu Dhabi',...}
#   vi. wrap it within curly braces to turn each pair into a dict,
#        and add as a new object within list_dict
#       where aim was to get len(list_dict) = (sub-)key-(sub-)value pair in the 'value' field of dict_collections
#   vii. back to outer loop
#   viii. insert the list of dictionaries into mongoDB, e.g. insert_many(list_dict)
#          under collection specified by the key field in data_collections
for key, value in dict_collections.items():
    list_dict = []
    for key_sub, value_sub in value.items():
        list_dict.append({key_sub: value_sub})
    db[key].insert_many(list_dict)
