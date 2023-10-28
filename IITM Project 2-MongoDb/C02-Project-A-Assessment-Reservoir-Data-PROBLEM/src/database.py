from pymongo import MongoClient

class Database:
    HOST = '127.0.0.1'
    PORT = '27017'
    DB_NAME = 'reservoir_db'

    def __init__(self):
        self._db_conn = MongoClient(f'mongodb://{Database.HOST}:{Database.PORT}')
        self._db = self._db_conn[Database.DB_NAME]

    def get_single_data(self, collection, key):
        db_collection = self._db[collection]
        document = db_collection.find_one(key)
        return document

    def get_multiple_data(self, collection, key):
        db_collection = self._db[collection]
        documents = db_collection.find(key)
        return documents

    def insert_single_data(self, collection, data):
        db_collection = self._db[collection]
        document = db_collection.insert_one(data)
        return document.inserted_id

    def insert_multiple_data(self, collection, data):
        db_collection = self._db[collection]
        result = db_collection.insert_many(data)
        return result.inserted_ids

    def aggregate(self, collection, pipeline):
        db_collection = self._db[collection]
        documents = db_collection.aggregate(pipeline)
        return documents