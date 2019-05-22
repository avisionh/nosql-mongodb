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

# 1. Connect to Mongo instance
# desc: put connection code in a class so that it can be reused.
class Connect(object):
    @staticmethod    
    def get_connection():
        return MongoClient("mongodb://user_readwrite:xtl0jDE3rtbH7H6J@cluster-open-shard-00-00-kzzlc.mongodb.net:27017,cluster-open-shard-00-01-kzzlc.mongodb.net:27017,cluster-open-shard-00-02-kzzlc.mongodb.net:27017/test?replicaSet=cluster-open-shard-0&authSource=admin&ssl=true")

# 2. Call the class just created.
client = Connect.get_connection()

# 3. Access the 'library' database
db = client.library

# 4. Retrieve all documents in 'authors' collection within 'library database
cursor = db.authors.find({})
for authors in cursor:
     pprint(authors)