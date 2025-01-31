# ETL-task

This script executes an ETL (Extract, Transform, Load) pipeline that cleans and aggregates customer transaction records in CSV files. It processes the transaction data, cleans and transforms it in to an aggregated dataset.

SEE INPUT CSV FILE: 'input_data.csv'

The data is cleaned by:
- checking if the input file is empty
- removes rows with missing values or duplicates
- converts numerical values in 'transaction_amount' column to float point values

The data is then transformed by:
- grouping the data by the customer_id and calculating the resulting total transaction amount per customer.

OUTPUT FILES:
Cleaned data: 'cleaned_data.csv'
Final transformed dataset: 'aggregated_transactions.csv'

See also:
Unit test file: 
