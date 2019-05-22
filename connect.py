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
        return MongoClient("mongodb://user_readwrite:<password>@cluster-open-shard-00-00-kzzlc.mongodb.net:27017,cluster-open-shard-00-01-kzzlc.mongodb.net:27017,cluster-open-shard-00-02-kzzlc.mongodb.net:27017/test?replicaSet=cluster-open-shard-0&authSource=admin&ssl=true")

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
                                  unzip=True)

# 2. Import EPL data into Python
with open(path_folder + '/fpl_data_2018_2019.json') as data_football:
    file_data = json.load(data_football)

# 3. Create database and collection for new data
db = client.football
collection_england = db.england
# 3. Import EPL data into Mongo
collection_england.insert_one(file_data)

# 4. Check it has been imported
cursor = db.england.find({})
for england in cursor:
     pprint(england)