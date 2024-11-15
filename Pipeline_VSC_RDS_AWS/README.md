# S&P 500 Data Pipeline

This project provides a comprehensive data pipeline to manage, transform, and store data related to S&P 500 companies. The pipeline includes functionalities such as creating a table in a database, loading raw data, transforming it, and uploading the results to AWS S3.

## Table of Contents

1. [Overview](#overview)
2. [Features](#features)
3. [Requirements](#requirements)
4. [Setup](#setup)
5. [Usage](#usage)
6. [File Descriptions](#file-descriptions)
7. [Acknowledgments](#acknowledgments)

---

## Overview

The S&P 500 Data Pipeline is designed to:
- Automate the creation of database tables.
- Load raw data from a CSV file into a relational database (AWS RDS MySQL).
- Apply transformations, including handling null values, grouping, and pivoting.
- Store the transformed data into a separate table and upload it to AWS S3.

---

## Features

- **Table Creation**: Automates the creation of a database table for storing raw data.
- **Raw Data Loading**: Reads S&P 500 company data from a CSV file and loads it into the database.
- **Data Transformation**: Applies various transformations like handling null values, grouping, and merging.
- **AWS S3 Integration**: Uploads transformed data to an S3 bucket for external storage.

---

## Requirements

Ensure you have the following installed:

- Python 3.7+
- AWS account with S3 bucket access
- MySQL database hosted on AWS RDS
- Required Python packages (see `requirements.txt`)

---

## Setup

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd <repository-name>
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment variables**:
   - Create a `.env` file with the following:
     ```env
     HOST=<your-rds-host>
     DATABASE=<your-database-name>
     USER=<your-username>
     PASSWORD=<your-password>
     AWS_ACCESS_KEY_ID=<your-aws-access-key-id>
     AWS_SECRET_ACCESS_KEY=<your-aws-secret-access-key>
     BUCKET_NAME=<your-s3-bucket-name>
     FILE_NAME=<desired-s3-file-name>
     CSV_FILE_PATH=<path-to-your-csv-file>
     ```

---

## Usage

1. **Run the main script**:
   ```bash
   python main.py
   ```

2. The script performs the following steps:
   - Connects to the RDS database.
   - Creates the required table if not already present.
   - Loads raw data from the CSV file into the database.
   - Transforms the raw data.
   - Stores transformed data in a new table.
   - Uploads the transformed data to AWS S3.

---

## File Descriptions

- `create_table.py`: Creates the SP500 table in the database if it does not already exist.
- `loadraw.py`: Loads raw data from a CSV file into the SP500 table.
- `transform.py`: Handles data transformations such as handling null values, grouping, and merging.
- `connectRDS.py`: Manages the connection to the AWS RDS MySQL database.
- `S3.py`: Uploads transformed data to AWS S3.
- `main.py`: Orchestrates the entire pipeline, calling the necessary scripts in sequence.
- `requirements.txt`: Lists the required Python libraries.
- `sp500_companies.csv`: Sample CSV file with raw S&P 500 company data.

---

## Acknowledgments

This pipeline is inspired by the need to automate and streamline data management processes for financial datasets. Special thanks to the contributors and community for providing the tools and libraries used in this project.
