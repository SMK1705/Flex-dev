import pymysql
import os
from dotenv import load_dotenv

def connect_to_rds():
    host = os.getenv("HOST")
    port = 3306#os.getenv("PORT")  # Default MySQL port
    database = os.getenv("DATABASE")
    user = os.getenv("USER")
    password = os.getenv("PASSWORD")

    try:
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port
        )
        print("Connection to AWS RDS MySQL successful")
        return connection

    except Exception as e:
        print(f"Error connecting to AWS RDS: {e}")
        return None

#connect_to_rds()

