"""
Database Connection Test Script

This script establishes a connection to the database using SQLAlchemy,
executes a simple query to fetch data from the 'tbl_daily_campaigns' table,
and prints the results to verify the database connection. It also handles 
exceptions during the query execution and ensures that the session is properly 
closed after the query.

Modules:
    - get_session: Retrieves a session object for interacting with the database.
    - close_session: Closes the current session to release database resources.
    - sqlalchemy.text: Allows the execution of raw SQL queries within SQLAlchemy.

Usage:
    Run this script to test the database connection and retrieve a sample of data 
    from the 'tbl_daily_campaigns' table. Ensure that the database credentials 
    and configurations are properly set up.

Note: This script assumes that a valid database connection and the necessary 
      table ('tbl_daily_campaigns') exist in the database.
"""

from database import get_session, close_session
from sqlalchemy import text

def test_connection():
    # Get a session object to interact with the database
    session = get_session()

    try:
        # Sample query to check the connection (fetching 5 rows from tbl_daily_campaigns)
        result = session.execute(text("SELECT * FROM tbl_daily_scores LIMIT 5"))
        
        # Iterate through the results and print them
        print("Test Query Results:")
        print(result.keys())
        for row in result:
            print(row)

    except Exception as e:
        print("Error during query execution:", e)
    finally:
        # Close the session
        close_session()

# Run the test
if __name__ == "__main__":
    test_connection()
