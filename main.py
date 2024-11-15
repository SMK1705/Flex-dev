import logging
from connectRDS import connect_to_rds
from create_table import create_sp500_table
from loadraw import load_data_into_sp500
from transform import load_raw_data, apply_transformations, load_transformed_data
from S3 import upload_to_s3
import os
from dotenv import load_dotenv

load_dotenv()

# Set up logging for the main script
logging.basicConfig(filename='process_log.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    try:
        logging.info("Process started.")
        
        # Step 1: Connect to RDS (verify connection, no need to use the connection here directly)
        connection = connect_to_rds()
        if connection:
            logging.info("Successfully connected to RDS from main.")
            connection.close()
        else:
            logging.error("Failed to connect to RDS.")
            return

        #s3_client = boto3.client('s3',aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"), aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"))
        
        # Step 2: Create the SP500 table
        create_sp500_table()
        logging.info("SP500 table created successfully in RDS.")
        
        # Step 3: Load raw data from CSV into the SP500 table
        csv_file_path = os.getenv("CSV_FILE_PATH")
        load_data_into_sp500(csv_file_path)
        logging.info("Raw data loaded into SP500 table successfully.")
        
        # Step 4: Load raw data from SP500 table for transformation
        raw_data = load_raw_data()
        if raw_data is not None:
            logging.info("Loaded raw data for transformation.")
        else:
            logging.error("Failed to load raw data for transformation.")
            return
        
        # Step 5: Apply transformations to raw data
        transformed_data = apply_transformations(raw_data)
        logging.info("Data transformations applied successfully.")
        
        # Step 6: Load transformed data into a new table in RDS
        load_transformed_data(transformed_data)
        logging.info("Transformed data loaded into SP500_transformed table successfully.")

        # Step 7: Upload transformed data to S3
        bucket_name = os.getenv("BUCKET_NAME")  # Specify your S3 bucket name
        file_name = os.getenv("FILE_NAME")   # Desired file name in S3
        upload_to_s3(transformed_data, bucket_name, file_name)
        logging.info("Transformed data uploaded to S3 successfully.")
        
        logging.info("Process completed successfully.")

    
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
