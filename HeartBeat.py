import os
import logging
import psycopg2
from psycopg2 import OperationalError

# Initialize logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    # Database connection parameters from environment variables
    db_params = {
        'host': os.environ['DB_HOST'],
        'port': os.environ.get('DB_PORT', '5432'),
        'database': os.environ['DB_NAME'],
        'user': os.environ['DB_USER'],
        'password': os.environ['DB_PASSWORD']
    }
    
    logger.info(f"Attempting to connect to database at {db_params['host']}")
    
    connection = None
    try:
        # Attempt to establish a connection
        connection = psycopg2.connect(**db_params)
        
        # Test the connection with a simple query
        with connection.cursor() as cursor:
            cursor.execute("SELECT version();")
            db_version = cursor.fetchone()
            logger.info(f"Successfully connected to PostgreSQL. Version: {db_version[0]}")
            
            # Additional test query to verify database operations
            cursor.execute("SELECT current_timestamp;")
            current_time = cursor.fetchone()
            logger.info(f"Database current timestamp: {current_time[0]}")
            
        return {
            'statusCode': 200,
            'body': 'Successfully connected to Aurora PostgreSQL',
            'version': db_version[0],
            'timestamp': str(current_time[0])
        }
        
    except OperationalError as e:
        error_msg = f"Connection failed: {str(e)}"
        logger.error(error_msg)
        return {
            'statusCode': 500,
            'body': error_msg
        }
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(error_msg)
        return {
            'statusCode': 500,
            'body': error_msg
        }
    finally:
        if connection:
            connection.close()
            logger.info("Database connection closed")