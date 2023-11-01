from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()

user = os.getenv('user_mongo')
pw = os.getenv('pass_mongo')
ip = os.getenv('ip_mongo')

url_mongo_sv = f"mongodb://{user}:{pw}@{ip}:27017/?authMechanism=DEFAULT"

client = MongoClient(url_mongo_sv)
db = client['data_tiki']
collection = db['products']

