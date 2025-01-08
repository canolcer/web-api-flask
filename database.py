"""
SQLAlchemy Database Connection and Session Management

This module provides the necessary configuration to connect to a MySQL database
and interact with it using SQLAlchemy ORM. It includes functions for creating
a session, managing the session lifecycle, and setting up the database connection.

Key Components:

1. **Database URL**: A connection string that contains the credentials and details
   required to connect to the MySQL database. The string includes the username, 
   password, host, port, and database name.

2. **SQLAlchemy Engine**: An SQLAlchemy engine is created using the provided database URL
   to establish a connection to the MySQL database.

3. **Session Maker**: The sessionmaker is bound to the engine, providing a session class
   that will be used to interact with the database.

4. **Base Class for ORM Models**: The `Base` class is provided, which is used to
   define ORM models (tables) in the database. All model classes must inherit from `Base`.

Functions:
- `get_session()`: Returns an active database session to interact with the database.
- `close_session()`: Closes the active session after database interactions are completed.

This module is designed to manage database connections and sessions for applications
interacting with a MySQL database using SQLAlchemy ORM. It abstracts away the complexity
of session management and provides a clean interface for database operations.
"""

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Database credentials and connection details
DATABASE_URL = "mysql+mysqlconnector://hr-task-user:adinTask2024!@hr-task-db.cqbarc8xc1jj.us-east-1.rds.amazonaws.com:3306/hr-task"

# Create SQLAlchemy engine
engine = create_engine(DATABASE_URL)

# Create sessionmaker and session
Session = sessionmaker(bind=engine)
session = Session()

# Base class for ORM models
Base = declarative_base()

def get_session():
    """Function to get a session for database interactions."""
    return session

def close_session():
    """Function to close the session."""
    session.close()
