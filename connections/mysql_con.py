import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

db_config = {
    'user': os.getenv('user'),
    'password': os.getenv('password'),
    'host': os.getenv('host'),
    'database': os.getenv('database'),
    'port': os.getenv('port'),
}

connection = mysql.connector.connect(**db_config)
cursor = connection.cursor()
