# create_table.py
from connectRDS import connect_to_rds

def create_sp500_table():
    connection = connect_to_rds()
    if connection:
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES LIKE 'SP500';")
        result = cursor.fetchone()
        
        if result:
            print("Table SP500 already exists. Skipping creation.")
        else:
            # Table does not exist, so create it
        #cursor.execute("DROP TABLE IF EXISTS SP500")  # Drop table if it exists
            cursor.execute("""
                CREATE TABLE SP500 (
                    Exchange VARCHAR(20),
                    Symbol VARCHAR(20),
                    Shortname VARCHAR(20),
                    Longname VARCHAR(20),
                    Sector VARCHAR(20),
                    Industry VARCHAR(20),
                    Currentprice FLOAT,
                    Marketcap BIGINT,
                    Ebitda FLOAT,
                    Revenuegrowth FLOAT,
                    City VARCHAR(20),
                    State VARCHAR(20),
                    Country VARCHAR(20),
                    Fulltimeemployees BIGINT,
                    Longbusinesssummary VARCHAR(255),
                    Weight FLOAT
                );
            """)
            print("Table creation: successful")
            cursor.close()
            #connection.close()
    else:
        print("Table creation: unsuccessful")

# Run the function to create the table
if __name__ == "__main__":
    create_sp500_table()
