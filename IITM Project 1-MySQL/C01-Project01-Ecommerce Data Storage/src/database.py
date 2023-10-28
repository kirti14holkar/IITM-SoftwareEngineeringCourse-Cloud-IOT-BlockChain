#File Created By Kirti
import mysql.connector
import random
import time
import datetime


# Global methods to push interact with the Database

# This method establishes the connection with the MySQL
def create_server_connection(host_name, user_name, user_password):
    # Implement the logic to create the server connection
    try:
        conn = mysql.connector.connect(host=host_name, user=user_name, password=user_password)
        return conn
    except Exception as e:
        print("Connection Failed ! ", e)


# This method will create the database and make it an active database
def create_and_switch_database(connection, db_name, switch_db):
    # For database creatio nuse this method
    # If you have created your databse using UI, no need to implement anything
    try:
        myCursor = connection.cursor()
        myCursor.execute("drop database IF EXISTS " + db_name)
        myCursor.execute("create database " + db_name)
        myCursor.execute("use " + db_name)
        print("Using " + db_name)
    except Exception as e:
        print("Operation Failed ! ", e)


# This method will establish the connection with the newly created DB
def create_db_connection(host_name, user_name, user_password, db_name):
    try:
        conn = mysql.connector.connect(host=host_name, user=user_name, password=user_password, database=db_name)
        return conn
    except Exception as e:
        print("Connection Failed ! ", e)


def drop_table(connection, table):
    try:
        myCursor = connection.cursor()
        myCursor.execute("drop table IF EXISTS " + table)
        print(table + " Table Dropped Successfully !\n")
    except Exception as e:
        print("Operation Failed ! ", e)


# Use this function to create the tables in a database
def create_table(connection, table, table_creation_statement):
    try:
        myCursor = connection.cursor()
        myCursor.execute(table_creation_statement)
        print(table + " Table Created Successfully !\n")
    except Exception as e:
        print("Operation Failed ! ", e)


# Perform all single insert statments in the specific table through a single function call
def create_insert_query(connection, table, query):
    # This method will perform creation of the table
    # this can also be used to perform single data point insertion in the desired table
    try:
        myCursor = connection.cursor()
        myCursor.execute(query)
        connection.commit()
        print("Record Inserted Successfully to " + table + " table !\n")
    except Exception as e:
        print("Operation Failed ! ", e)


# retrieving the data from the table based on the given query
def select_query(connection, query):
    try:
        # fetching the data points from the table
        myCursor = connection.cursor()
        myCursor.execute(query)
        myRecords = myCursor.fetchall()
        return myRecords
    except Exception as e:
        print("Operation Failed ! ", e)


# Execute multiple insert statements in a table
def insert_many_records(connection, table, sql, val):
    try:
        myCursor = connection.cursor()
        myCursor.executemany(sql, val)
        connection.commit()
        print("Records Inserted Successfully to " + table + " table !\n")
    except Exception as e:
        print("Operation Failed ! ", e)