import os
import logging
import psycopg2
import boto3
from botocore.exceptions import ClientError
from psycopg2 import OperationalError
from datetime import datetime

# Initialize logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create boto3 clients
logs_client = boto3.client('logs')
secrets_client = boto3.client('secretsmanager')

# Custom CloudWatch Log Group Configuration
CUSTOM_LOG_GROUP = '/aws/custom/aurora-connectivity'
LOG_STREAM_PREFIX = 'execution-'

def create_custom_log_stream():
    """Create a custom log stream in CloudWatch"""
    try:
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        log_stream_name = f"{LOG_STREAM_PREFIX}{timestamp}"
        
        # Create log group if it doesn't exist
        try:
            logs_client.create_log_group(logGroupName=CUSTOM_LOG_GROUP)
            logs_client.put_retention_policy(
                logGroupName=CUSTOM_LOG_GROUP,
                retentionInDays=7
            )
        except logs_client.exceptions.ResourceAlreadyExistsException:
            pass
            
        # Create log stream
        logs_client.create_log_stream(
            logGroupName=CUSTOM_LOG_GROUP,
            logStreamName=log_stream_name
        )
        return log_stream_name
    except Exception as e:
        logger.error(f"Failed to create custom log stream: {str(e)}")
        return None

def log_to_custom_cloudwatch(message, level='INFO', log_stream_name=None):
    """Log messages to custom CloudWatch log group"""
    if not log_stream_name:
        return
        
    try:
        timestamp = int(datetime.now().timestamp() * 1000)
        
        logs_client.put_log_events(
            logGroupName=CUSTOM_LOG_GROUP,
            logStreamName=log_stream_name,
            logEvents=[
                {
                    'timestamp': timestamp,
                    'message': f"[{level}] {message}"
                }
            ]
        )
    except Exception as e:
        logger.error(f"Failed to write to custom CloudWatch: {str(e)}")

def get_db_secrets(secret_name):
    """Retrieve database credentials from AWS Secrets Manager"""
    try:
        logger.info(f"Retrieving secrets for: {secret_name}")
        response = secrets_client.get_secret_value(SecretId=secret_name)
        
        if 'SecretString' in response:
            secrets = eval(response['SecretString'])
            logger.info("Successfully retrieved database secrets")
            return secrets
        else:
            raise Exception("Secret binary not supported")
            
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_msg = f"Secrets Manager error ({error_code}): {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)
    except Exception as e:
        error_msg = f"Unexpected secrets error: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)

def test_db_connection(db_params, log_stream_name):
    """Test the database connection with provided parameters"""
    connection = None
    test_record = None
    try:
        log_msg = f"Attempting to connect to database at {db_params['host']}"
        logger.info(log_msg)
        log_to_custom_cloudwatch(log_msg, 'INFO', log_stream_name)
        
        connection = psycopg2.connect(**db_params)
        
        with connection.cursor() as cursor:
            # Test basic query
            cursor.execute("SELECT version();")
            db_version = cursor.fetchone()
            version_msg = f"Database version: {db_version[0]}"
            logger.info(version_msg)
            log_to_custom_cloudwatch(version_msg, 'INFO', log_stream_name)
            
            # Create test table
            cursor.execute("""
                CREATE TEMP TABLE IF NOT EXISTS lambda_test (
                    id serial PRIMARY KEY, 
                    timestamp timestamp,
                    test_value text
                );
            """)
            
            # Insert test record
            cursor.execute("""
                INSERT INTO lambda_test (timestamp, test_value) 
                VALUES (current_timestamp, 'Lambda connectivity test') 
                RETURNING id, timestamp, test_value;
            """)
            test_record = cursor.fetchone()
            record_msg = f"Test record inserted: {test_record}"
            logger.info(record_msg)
            log_to_custom_cloudwatch(record_msg, 'INFO', log_stream_name)
            
            # Verify the record exists
            cursor.execute("SELECT * FROM lambda_test WHERE id = %s;", (test_record[0],))
            verify_record = cursor.fetchone()
            verify_msg = f"Record verification: {verify_record}"
            logger.info(verify_msg)
            log_to_custom_cloudwatch(verify_msg, 'INFO', log_stream_name)
            
            # Delete the test record
            if test_record:
                cursor.execute("DELETE FROM lambda_test WHERE id = %s;", (test_record[0],))
                delete_msg = f"Deleted test record with ID: {test_record[0]}"
                logger.info(delete_msg)
                log_to_custom_cloudwatch(delete_msg, 'INFO', log_stream_name)
                
                # Verify deletion
                cursor.execute("SELECT * FROM lambda_test WHERE id = %s;", (test_record[0],))
                verify_deletion = cursor.fetchone()
                if not verify_deletion:
                    logger.info("Record successfully deleted")
                else:
                    logger.warning("Record still exists after deletion")
            
            connection.commit()
            
            return {
                'status': 'success',
                'version': db_version[0],
                'test_record': test_record,
                'deleted_record_id': test_record[0] if test_record else None
            }
            
    except OperationalError as e:
        error_msg = f"Database connection failed: {str(e)}"
        logger.error(error_msg)
        log_to_custom_cloudwatch(error_msg, 'ERROR', log_stream_name)
        return {
            'status': 'failed',
            'error': error_msg
        }
    except Exception as e:
        error_msg = f"Database operation failed: {str(e)}"
        logger.error(error_msg)
        log_to_custom_cloudwatch(error_msg, 'ERROR', log_stream_name)
        return {
            'status': 'failed',
            'error': error_msg
        }
    finally:
        if connection:
            connection.close()
            logger.info("Database connection closed")

def lambda_handler(event, context):
    # Create custom log stream
    log_stream_name = create_custom_log_stream()
    
    try:
        # Get secret name from environment variable
        secret_name = os.environ['DB_SECRET_NAME']
        
        # Retrieve database credentials from Secrets Manager
        secrets = get_db_secrets(secret_name)
        
        # Prepare connection parameters
        db_params = {
            'host': secrets['host'],
            'port': secrets.get('port', '5432'),
            'database': secrets['dbname'],
            'user': secrets['username'],
            'password': secrets['password'],
            'connect_timeout': 5  # 5 seconds connection timeout
        }
        
        # Test the connection
        result = test_db_connection(db_params, log_stream_name)
        
        if result['status'] == 'success':
            success_msg = "Successfully connected to and tested Aurora PostgreSQL"
            logger.info(success_msg)
            log_to_custom_cloudwatch(success_msg, 'INFO', log_stream_name)
            
            return {
                'statusCode': 200,
                'body': {
                    'message': success_msg,
                    'version': result['version'],
                    'test_record_inserted': result['test_record'],
                    'deleted_record_id': result['deleted_record_id'],
                    'log_stream': log_stream_name
                }
            }
        else:
            logger.error("Failed to connect to database")
            return {
                'statusCode': 500,
                'body': {
                    'message': 'Failed to connect to database',
                    'error': result['error'],
                    'log_stream': log_stream_name
                }
            }
            
    except Exception as e:
        error_msg = f"Lambda execution failed: {str(e)}"
        logger.error(error_msg)
        log_to_custom_cloudwatch(error_msg, 'ERROR', log_stream_name)
        
        return {
            'statusCode': 500,
            'body': {
                'message': 'Lambda execution error',
                'error': error_msg,
                'log_stream': log_stream_name
            }
        }
