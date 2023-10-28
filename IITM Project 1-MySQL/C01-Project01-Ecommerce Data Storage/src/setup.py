#File Created By Kirti
import csv
import database as db

UserConf = input(
    'This Operation would drop the existing database and its tables, Are you sure you want to continue ? (Y/N) : ')

if (UserConf == 'Y' or UserConf == 'y'):

    print('DB Setup Begins...')

    PW = "root"  # IMPORTANT! Put your MySQL Terminal password here.
    ROOT = "root"
    DB = "ecommerce_record"  # This is the name of the database we will create in the next step - call it whatever you like.
    LOCALHOST = "localhost"  # considering you have installed MySQL server on your computer

    RELATIVE_CONFIG_PATH = '../config/'

    USER = 'users'
    PRODUCTS = 'products'
    ORDER = 'orders'

    connection = db.create_server_connection(LOCALHOST, ROOT, PW)

    # creating the schema in the DB
    db.create_and_switch_database(connection, DB, DB)

    # Create the tables through python code here
    # if you have created the table in UI, then no need to define the table structure
    # If you are using python to create the tables, call the relevant query to complete the creation

    with open(RELATIVE_CONFIG_PATH + USER + '.csv', 'r') as f:
        val = []
        data = csv.reader(f)
        for row in data:
            val.append(tuple(row))
        val.pop(0)

        """
        Here we have accessed the file data and saved into the val data struture, which list of tuples. 
        Now you should call appropriate method to perform the insert operation in the database. 
        """

        ## Drop User Table if exists
        db.drop_table(connection, "users")

        ## Create User Table.
        UserTable = "create table users (user_id varchar(10) PRIMARY KEY,user_email VARCHAR(100),user_name VARCHAR(100),user_password VARCHAR(100),user_address VARCHAR(255),is_vendor TINYINT DEFAULT 0 CHECK(is_vendor=0 or is_vendor=1));"
        db.create_table(connection, USER, UserTable)

        ## Create and executey SQL Statement to insert records
        SQLInsert = "Insert into " + USER + "(user_id,user_email,user_name,user_password,user_address,is_vendor) " + "values(%s,%s,%s,%s,%s,%s)"
        db.insert_many_records(connection, USER, SQLInsert, val)

    with open(RELATIVE_CONFIG_PATH + PRODUCTS + '.csv', 'r') as f:
        val = []
        data = csv.reader(f)
        for row in data:
            val.append(tuple(row))
        val.pop(0)
        """
        Here we have accessed the file data and saved into the val data struture, which list of tuples. 
        Now you should call appropriate method to perform the insert operation in the database. 
        """

        ## Drop User Table if exists
        db.drop_table(connection, "products")

        ##Create Product Table
        ProductTable = "create table products (product_id varchar(10) PRIMARY KEY,product_name varchar(100),product_description varchar(255),vendor_id varchar(10), product_price FLOAT,emi_available VARCHAR(50),FOREIGN KEY (vendor_id) REFERENCES users(user_id));"
        db.create_table(connection, PRODUCTS, ProductTable)

        ## Create and execute SQL Statement to insert records
        SQLInsert = "Insert into " + PRODUCTS + " (product_id,product_name,product_price,product_description,vendor_id,emi_available) " + "values(%s,%s,%s,%s,%s,%s)"
        db.insert_many_records(connection, PRODUCTS, SQLInsert, val)

    with open(RELATIVE_CONFIG_PATH + ORDER + '.csv', 'r') as f:
        val = []
        data = csv.reader(f)
        for row in data:
            val.append(tuple(row))

        val.pop(0)
        """
        Here we have accessed the file data and saved into the val data struture, which list of tuples. 
        Now you should call appropriate method to perform the insert operation in the database. 
        """

        ## Drop User Table if exists
        db.drop_table(connection, "orders")

        ##Create Order Table
        OrderTable = "create table orders (order_id INT PRIMARY KEY,total_value FLOAT,customer_id varchar(10),vendor_id varchar(10),order_quantity INT,reward_point INT,FOREIGN KEY (customer_id) REFERENCES users(user_id),FOREIGN KEY (vendor_id) REFERENCES users(user_id));"
        db.create_table(connection, ORDER, OrderTable)

        ## Create and execute SQL Statement to insert records
        SQLInsert = "Insert into " + ORDER + " (order_id,customer_id,vendor_id,total_value,order_quantity,reward_point) " + "values(%s,%s,%s,%s,%s,%s)"
        db.insert_many_records(connection, ORDER, SQLInsert, val)

        print('DB Setup Completed !')

        connection.close()
else:

    print('DB Setup Aborted!')


