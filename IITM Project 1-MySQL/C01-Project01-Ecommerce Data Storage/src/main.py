#File Created By Kirti
import database as db

# Driver code
if __name__ == "__main__":
    """
    Please enter the necessary information related to the DB at this place. 
    Please change PW and ROOT based on the configuration of your own system. 
    """
    PW = "root"  # IMPORTANT! Put your MySQL Terminal password here.
    ROOT = "root"
    DB = "ecommerce_record"  # This is the name of the database we will create in the next step - call it whatever
    # you like.
    LOCALHOST = "localhost"
    connection = db.create_db_connection(LOCALHOST, ROOT, PW, DB)

    # creating the schema in the DB (Not Required as already created database during Setup)
    # db.create_and_switch_database(connection, DB, DB)

    # Start implementing your task as mentioned in the problem statement
    # Implement all the test cases and test them by running this file

    # 2.b. Insert 5 records for different Customers
    InsertQueryOrders = "Insert into orders (order_id,customer_id,vendor_id,total_value,order_quantity,reward_point) " + "values(%s,%s,%s,%s,%s,%s)"
    OrderVal = [(101, 11, 1, 1234, 1, 50), (102, 12, 2, 5678, 1, 50), (103, 13, 3, 91011, 1, 100),
                (104, 14, 4, 12134, 1, 100), (105, 15, 5, 15167, 1, 100)]
    db.insert_many_records(connection, "Orders", InsertQueryOrders, OrderVal)

    # 2.c. Print all records from Orders table.
    OrderTableSelectQuery = "Select * from Orders"
    OrderTableResult = db.select_query(connection, OrderTableSelectQuery)
    print('Printing Order Table Records :')
    for Row in OrderTableResult:
        print(Row)
    print("\n\n")

    # 3.a. Find Maximum and Minimum order value from Orders Table.
    # Query Database
    MinMaxQuery = "select max(total_value) as max_order_value,min(total_value) as min_order_value from orders;"
    MinMaxResult = db.select_query(connection, MinMaxQuery)
    # Print Output
    print(
        "Maximum Order Value : {} and Minimum Order Value : {}".format(MinMaxResult[0][0], MinMaxResult[0][1]) + "\n\n")

    # 3.b. All Total_Value Greater than Average of Total Value in orders table
    # Query Database
    GtrAvgQuery = "select total_value from orders where total_value > (select avg(total_value) from orders)"
    GtrAvgResult = db.select_query(connection, GtrAvgQuery)
    # Print Output
    print("Total_Value Greater than Average of Total Value is : ")
    for Row in GtrAvgResult:
        for Col in Row:
            print(Col)
    print("\n\n")

    # 3.c. Create Table Customer_Leaderboard and populate table with highest order value for each customer.
    # Drop Table if exist and Create Table Customer_Leaderboard
    CustLBCreateQuery = "create table customer_leaderboard (customer_id varchar(10), total_value float,customer_name varchar(100), customer_email varchar(100));"
    db.drop_table(connection, "customer_leaderboard")
    db.create_table(connection, "customer_leaderboard", CustLBCreateQuery)

    # Insert into customer leader board wih with highest order value for each customer. There are 2 ways to achieve this as below.

    ## Method 1 Direct SQL to select and insert records. Which is most efficient way as it does not need to bring bring back results to Python client over the network.
    # InsertAndSelectQuery = "insert into customer_leaderboard select a.customer_id,max(a.total_value),b.user_name,b.user_email from orders a inner join users b on a.customer_id = b.user_id group by a.customer_id,b.user_name,b.user_email"
    # db.create_insert_query(connection, "customer_leaderboard", InsertAndSelectQuery)

    ## Method 2 Get the records with Select to Python Client and then insert the result to table.
    # Select Query to fetch records
    SelectQuery = "select a.customer_id,max(a.total_value),b.user_name,b.user_email from orders a inner join users b on a.customer_id = b.user_id group by a.customer_id,b.user_name,b.user_email"
    SelectQueryResult = db.select_query(connection, SelectQuery)
    # Create and execute SQL Statement to insert records
    SQLInsert = "Insert into customer_leaderboard (customer_id,total_value,customer_name,customer_email) values(%s,%s,%s,%s)"
    db.insert_many_records(connection, "customer_leaderboard", SQLInsert, SelectQueryResult)

    # 2Print all records from Customer Leaderboard table.
    CLSelectQuery = "Select * from customer_leaderboard order by total_value desc"
    CLResult = db.select_query(connection, CLSelectQuery)
    print('Printing customer_leaderboard Table Records :')
    for Row in CLResult:
        print(Row)
    print("\n\n")


