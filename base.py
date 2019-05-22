# - MongoDB x Python - #
# Description: This script demonstrates the following:
#   1. Connecting to MongoDB Atlas
#   2. Writing data to a database 
#   3. Reading data from a database
#   4. Analysing data from a database
# Note: The password for READ-WRITE access to this database is hidden in this script.
#       Please request the password from a_vision@hotmail.co.uk  

from pymongo import MongoClient

class Connect(object):
    @staticmethod    
    def get_connection():
        return MongoClient("mongodb://admin_readwrite:xtl0jDE3rtbH7H6J@cluster-open-shard-00-00-kzzlc.mongodb.net:27017,cluster-open-shard-00-01-kzzlc.mongodb.net:27017,cluster-open-shard-00-02-kzzlc.mongodb.net:27017/test?replicaSet=cluster-open-shard-0&authSource=admin&ssl=true")



client = Connect.get_connection()

db = client.library

cursor = db.books.find({})

from pprint import pprint

for books in cursor:
     pprint(books)