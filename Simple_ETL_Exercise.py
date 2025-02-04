import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from hashlib import md5

"""
This performs the ETL task in a simple way without classes/functions.
"""

# reading in the input CSV
try:
    input_data = pd.read_csv('input_data.csv', delimiter=',')
    if input_data.empty:
        print("Error: file is empty.")
except FileNotFoundError:
    print("Error: file not found.")
    
# remove rows with missing values
input_data.dropna(inplace=True)

# convert transaction_amount column to float64
input_data['transaction_amount'] = pd.to_numeric(
    input_data['transaction_amount'], errors='raise'
)

# check for duplicates by creating unique IDs

# create columns list
columns = input_data.columns.tolist()

# generating unique ID
input_data['unique_id'] = input_data.apply(
            lambda row: md5(
                "_".join(
                    str(row[col]).strip().lower()
                    for col in columns
                    if pd.notna(row[col])
                ).encode()
            ).hexdigest(),
            axis=1
        )
# drop duplicates
input_data.drop_duplicates(subset=['unique_id'], keep='first')

# group data by customer_id and find total transaction amount for each
groupby_data = input_data.groupby(
            'customer_id', as_index=False
        )['transaction_amount'].sum()
groupby_data.rename(
            columns={'transaction_amount': 'total_transaction_amount'},
            inplace=True
        )

# now save the groupby_data to a CSV file for output
groupby_data.to_csv('aggregated_transactions.csv', index=False)
