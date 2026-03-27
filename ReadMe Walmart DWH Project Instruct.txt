ReadMe: Walmart DWH Project Instructions
This document provides step-by-step instructions to set up, populate, and query the Walmart Data Warehouse.

1. Prerequisites

Before you begin, ensure you have the following software installed:
Python 3.x
A running MySQL Server (e.g., MySQL Community Server)
A MySQL client (e.g., MySQL Workbench)
You will also need the following Python libraries. You can install them using pip:
pip install pandas sqlalchemy pymysql


2. Setup: Place Data Files

Ensure the following three (3) CSV files are in the same folder as the Hybrid-Join.py script:
customer_master_data.csv
product_master_data.csv
transactional_data.csv




3. Step 1: Create the Database Schema

You must create the database and all the tables before running the Python script.
Open your MySQL client (e.g., MySQL Workbench).
Open the Create-DW-Schema.sql file.
Run the entire script. This will:
Create the walmart_warehouse database.
Create all 5 dimension tables (e.g., dim_customer, dim_product).
Create the fact_sales table.



4. Step 2: Run the ETL Script (Load Data)

This script will prompt you for your database credentials, load the dimensions, and then populate the fact_sales table.
Open your terminal or command prompt.
Navigate to the project directory (where app.py is located).
Run the script:
python app.py

When prompted, enter your MySQL database credentials. The defaults are shown in parentheses:
Username (default: root): (Press Enter to use root)
Password for root: (Type your password and press Enter. It will be hidden.)
Host (default: localhost): (Press Enter)
Port (default: 3306): (Press Enter)
Database Name (default: walmart_warehouse): (Press Enter)

The script will connect and begin the ETL process:
First, it loads all dimension tables 
Then, it processes the transaction file using HYBRIDJOIN 




5. Step 3: Run Analysis Queries

Once the "ETL process finished." message appears, your data warehouse is fully loaded and indexed.
Go back to your MySQL client (MySQL Workbench)Run any query to analyze the data. They should now return results in a few seconds.

