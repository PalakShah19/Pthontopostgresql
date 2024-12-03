import psycopg2

def connect_to_database():
    """Establish a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
           
        host="localhost",
        database="projectdbms",  
        user="postgres",
        password="112233",
        port="5432"

        )
        conn.autocommit = False  # Ensure manual transaction handling
        print("Connected to the database successfully.")
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def execute_transaction(conn, queries):
    """
    Execute a list of SQL queries as a single transaction.
    :param conn: Active database connection
    :param queries: List of SQL queries to execute
    """
    try:
        with conn.cursor() as cursor:
            for query in queries:
                cursor.execute(query)
            conn.commit()  # Commit the transaction if all queries succeed
            print("Transaction executed successfully.")
    except Exception as e:
        conn.rollback()  # Rollback the transaction if an error occurs
        print(f"Transaction failed. Rolling back changes. Error: {e}")

if __name__ == "__main__":
    # Connect to the PostgreSQL database
    conn = connect_to_database()
    if conn:
        # Define the transactions to be executed
        
     transactions = [
          # 3. Rename product p1 to pp1 in Product and Stock
    [
        """
        UPDATE Stock SET prod_id = 'pp1' WHERE prod_id = 'p1';
        """,
        """
        UPDATE Product SET prod_id = 'pp1' WHERE prod_id = 'p1';
        """
    ],
        # 4. Rename depot d1 to dd1 in Depot and Stock
    [
        """
        UPDATE Stock SET dep_id = 'dd1' WHERE dep_id = 'd1';
        """,
        """
        UPDATE Depot SET dep_id = 'dd1' WHERE dep_id = 'd1';
        """
    ],

        # 5. Add a product (p100, cd, 5) in Product and (p100, d2, 50) in Stock
    [
        """
        INSERT INTO Product (prod_id, pname, price) 
        SELECT 'p100', 'cd', 5 
        WHERE NOT EXISTS (SELECT 1 FROM Product WHERE prod_id = 'p100');
        """,
        """
        INSERT INTO Stock (prod_id, dep_id, quantity) 
        SELECT 'p100', 'd2', 50 
        WHERE NOT EXISTS (SELECT 1 FROM Stock WHERE prod_id = 'p100' AND dep_id = 'd2');
        """
    ],
        # 6. Add a depot (d100, Chicago, 100) in Depot and (p1, d100, 100) in Stock
    [
        """
        INSERT INTO Depot (dep_id, addr, volume) 
        SELECT 'd100', 'Chicago', 100 
        WHERE NOT EXISTS (SELECT 1 FROM Depot WHERE dep_id = 'd100');
        """,
        """
        INSERT INTO Stock (prod_id, dep_id, quantity) 
        SELECT 'p1', 'd100', 100 
        WHERE EXISTS (SELECT 1 FROM Product WHERE prod_id = 'p1') 
        AND NOT EXISTS (SELECT 1 FROM Stock WHERE prod_id = 'p1' AND dep_id = 'd100');
        """
    ],
    
        # 1. Delete product p1 from Product and Stock
    [
        "DELETE FROM Stock WHERE prod_id = 'p1';",
        "DELETE FROM Product WHERE prod_id = 'p1';"
    ],
        # 2. Delete depot d1 from Depot and Stock
    [
        "DELETE FROM Stock WHERE dep_id = 'd1';",
        "DELETE FROM Depot WHERE dep_id = 'd1';"
    ]
     ]

        # Execute each transaction in the specified order
for i, queries in enumerate(transactions, start=1):
            print(f"Executing Transaction {i}:")
            execute_transaction(conn, queries)

        # Close the connection
conn.close()
print("Database connection closed.")