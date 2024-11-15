import pandas as pd
import numpy as np
import logging
from connectRDS import connect_to_rds

# Set up logging
logging.basicConfig(filename='transformation_log.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

def load_raw_data():
    connection = connect_to_rds()
    if connection:
        query = "SELECT * FROM SP500"
        raw_data = pd.read_sql(query, connection)
        #connection.close()
        logging.info("Loaded raw data from SP500 table.")
        return raw_data
    else:
        logging.error("Failed to connect to RDS for loading raw data.")
        return None

def apply_transformations(data):
    # 1. Handle null values: Fill NaN with a placeholder or appropriate default values based on column type
    data = data.fillna(0 if np.issubdtype(data.dtypes, np.number) else 'Unknown')
    logging.info("Filled NaN values across all columns with 0 for numeric fields and 'Unknown' for non-numeric fields.")

    # 2. Groupby: Group by all categorical columns and calculate a sum for numeric columns
    categorical_columns = data.select_dtypes(include=['object']).columns
    numeric_columns = data.select_dtypes(include=[np.number]).columns
    if len(categorical_columns) > 0:
        grouped_data = data.groupby(list(categorical_columns)).sum(numeric_only=True).reset_index()
        logging.info(f"Grouped data by {list(categorical_columns)} and summed numeric columns.")
    else:
        grouped_data = data
        logging.warning("No categorical columns found for groupby operation.")

    # 3. Pivot example: Pivoting on the first categorical and numeric columns if possible
    if len(categorical_columns) > 0 and len(numeric_columns) > 0:
        pivot_data = data.pivot_table(values=numeric_columns[0], index=categorical_columns[0], fill_value=0)
        logging.info(f"Pivoted data on {categorical_columns[0]} with {numeric_columns[0]} as values.")
    else:
        pivot_data = data
        logging.warning("Unable to perform pivot as categorical or numeric columns are insufficient.")

    # 4. Melt example: Convert all columns to a long format
    melted_data = data.melt()
    logging.info("Melted the data frame to long format with all columns.")

    # 5. Stack example: Stack all columns in the DataFrame
    stacked_data = data.stack().reset_index()
    logging.info("Stacked all columns in the data to multi-level index format.")

    # 6. Merge example: Merge original data with grouped data
    merged_data = pd.merge(data, grouped_data, on=list(categorical_columns), how="outer", suffixes=('', '_grouped'))
    logging.info("Merged the original data with grouped data on all categorical columns.")

    # 7. Concatenate example: Concatenate data with itself for demonstration
    concat_data = pd.concat([data, data], ignore_index=True)
    logging.info("Concatenated data with itself along rows.")

    # 8. Union example: Perform union-like concatenation for demonstration purposes
    union_data = pd.concat([data, data.drop_duplicates()], ignore_index=True)
    logging.info("Performed a union of data by concatenating it with itself and removing duplicates.")

    # Choose one or more transformed datasets for final loading
    transformed_data = merged_data  # Selecting merged data as the final transformed set for loading
    logging.info("Selected merged data as the final transformed dataset for loading.")

    return transformed_data

def load_transformed_data(transformed_data):
    connection = connect_to_rds()
    if connection:
        cursor = connection.cursor()

        # Define the columns that the SQL table expects
        expected_columns = [
            'Exchange', 'Symbol', 'Shortname', 'Longname', 'Sector', 'Industry', 
            'Currentprice', 'Marketcap', 'Ebitda', 'Revenuegrowth', 
            'City', 'State', 'Country', 'Fulltimeemployees', 
            'Longbusinesssummary', 'Weight', 'Marketcap_grouped'
        ]

        # Ensure transformed_data has the expected columns only
        transformed_data = transformed_data[expected_columns]
        
        # Confirm column alignment
        if transformed_data.shape[1] != len(expected_columns):
            raise ValueError(f"Expected {len(expected_columns)} columns, but got {transformed_data.shape[1]}")

        # Create a new table for the transformed data
        cursor.execute("DROP TABLE IF EXISTS SP500_transformed")
        cursor.execute("""
            CREATE TABLE SP500_transformed (
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
                Weight FLOAT,
                Marketcap_grouped BIGINT
            );
        """)
        logging.info("Created table SP500_transformed in RDS.")

        # Replace NaN values with None in transformed data before loading
        transformed_data = transformed_data.replace({np.nan: None})

        # Insert transformed data into the new table
        insert_query = """
            INSERT INTO SP500_transformed (
                Exchange, Symbol, Shortname, Longname, Sector, Industry, 
                Currentprice, Marketcap, Ebitda, Revenuegrowth, 
                City, State, Country, Fulltimeemployees, 
                Longbusinesssummary, Weight, Marketcap_grouped
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        values_list = [tuple(row) for row in transformed_data.values]
        cursor.executemany(insert_query, values_list)
        connection.commit()
        cursor.close()
        #connection.close()
        logging.info("Transformed data loaded into SP500_transformed table successfully.")
    else:
        logging.error("Failed to connect to RDS for loading transformed data.")

if __name__ == "__main__":
    raw_data = load_raw_data()
    if raw_data is not None:
        transformed_data = apply_transformations(raw_data)
        load_transformed_data(transformed_data)
