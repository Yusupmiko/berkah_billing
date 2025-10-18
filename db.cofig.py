import mysql.connector

def get_db_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="",  # sesuaikan
        database="berkah_billing"  # sesuaikan dengan nama database kamu
    )
