import os
import pyodbc
import logging

logger = logging.getLogger(__name__)

def get_db_connection(max_retries=3, retry_delay=5):
    """Creates a connection to the NBA database using environment variables."""
    try:
        server = os.getenv('DB_SERVER')
        database = os.getenv('DB_NAME', 'NBA_Database')
        username = os.getenv('DB_USERNAME')
        password = os.getenv('DB_PASSWORD')
        
        if not all([server, username, password]):
            raise ValueError("Missing required database environment variables")
        
        connection_string = (
            'DRIVER={ODBC Driver 17 for SQL Server};'
            f'SERVER={server};'
            f'DATABASE={database};'
            f'UID={username};'
            f'PWD={password};'
            'Connection Timeout=30;'
            'Login Timeout=30;'
            'TrustServerCertificate=yes;'
            'MultipleActiveResultSets=true;'
            'Encrypt=yes'
        )
        
        # Try to establish connection with retry logic
        for attempt in range(max_retries):
            try:
                conn = pyodbc.connect(connection_string)
                logger.info(f"Database connection established successfully on attempt {attempt + 1}")
                return conn
            except pyodbc.Error as e:
                if attempt == max_retries - 1:
                    raise
                logger.warning(f"Connection attempt {attempt + 1} failed: {str(e)}")
                time.sleep(retry_delay)
                
    except Exception as e:
        logger.error(f"Failed to connect to database: {str(e)}")
        raise