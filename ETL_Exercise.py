import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from hashlib import md5



class CSVCleaner:
    
    """
    This class will take an input CSV and clean the data with the methods outlined:
    - check if the input file is empty
    - removing rows with missing values 
    - check for duplicate transactions
    - convert transaction_amount to a floating point number
    - saves the data

    We can add more methods here in future if needed.
    """

    
    
    def __init__(self, file_path: str):
        
        """
        Initialising class with a file path.
        """
        
        self.file_path = file_path
        self.data = None
        
        
        
    def read_data(self) -> pd.DataFrame:

        """
        Reading CSV file and checking whether it is empty.
        """
        
        try:
            self.data = pd.read_csv(self.file_path, delimiter=',')
            if self.data.empty:
                raise ValueError('The input CSV contains no data.')
        except FileNotFoundError:
            raise FileNotFoundError('File not found: {}'.format(self.file_path))
            
        return self.data
                        
            
            
    def remove_rows(self) -> pd.DataFrame:
        
        """
        Removing rows with missing values and prints how many were removed.
        """
        
        count_row = len(self.data)
        self.data.dropna(inplace=True)
        count_removed_rows = count_row - len(self.data)
        print('Removed {} rows with missing values.'.format(count_removed_rows))
            
        return self.data
        
        
    def generate_uniqueID(self, columns: list = None) -> pd.DataFrame:
        
        """
        In order to robustly check for duplicates, we are generating a unique ID
        per row. We are combining column data from each row in to a single joined string 
        and then using md5 hashing to create a unique ID for each row.
        More complex methods can be used than md5, but e.g. will take longer to run.
        
        If when calling the function, specific column names are not provided,
        the default will be using all of the columns.
        
        The unique IDs are added in another column at the end with the name 'unique_id'.
        """
        
        
        if columns is None:  
            columns = self.data.columns.tolist()
        
        self.data['unique_id'] = self.data.apply(lambda row: 
                                                 md5("_".join(str(row[col]).strip().lower() 
                                                for col in columns 
                                                if pd.notna(row[col])).encode()).hexdigest(), axis=1)
        
        return self.data
    
    
    
    def check_duplicates(self) -> pd.DataFrame:
        
        """
        Checking for duplicates in the new unique ID column, drops duplicate rows 
        and prints how many were dropped.
        """
        
        count_row = len(self.data)
        self.data.drop_duplicates(subset=['unique_id'], keep='first')
        count_removed_rows = count_row - len(self.data)
        print('Removed {} rows with duplicate transactions.'.format(count_removed_rows))
        
        return self.data
        
        
        
    def convert_transaction_amount(self) -> pd.DataFrame:
        
        """
        Converting the transaction_amount column to float using pd.to_numeric (default float64).
        Prints confirmation.
        """
        
        self.data['transaction_amount'] = pd.to_numeric(self.data['transaction_amount'],errors='raise')
        print('Converted transaction_amount values to float.')
    
        return self.data
    
    
    
    def save_data(self, output_file: str) -> pd.DataFrame:
        
        """
        Saving cleaned data into a new CSV output file. Prints filename when saved.
        """
        
        self.data.to_csv(output_file,index=False)
        print('Cleaned CSV saved as: {}'.format(output_file))
        
        return self.data
        
        
        
        

class Transformations:
    
    """
    This will take an input CSV (this should be the cleaned CSV) and perform the transformations outlined:
    - grouping the transactions by customer_id and calculating the total transaction 
      amount per customer
      
    We can add more transformation methods here in future if needed.
    """
        
            
    def __init__(self, file_path: str):
        
        """
        Initialises the class, with a file path.
        Reads in CSV.
        """
        
        self.file_path = file_path
        self.data = pd.read_csv(self.file_path, delimiter=',')
    
    
        
    def group_by_customer_id(self) -> pd.DataFrame:
        
        """
        Grouping by customer_id and the aggregate function is 
        calculating total transaction amount per customer.
        """
        
        groupby_data = self.data.groupby('customer_id',as_index=False)['transaction_amount'].sum()#.reset_index()
        groupby_data.rename(columns={'transaction_amount':'total_transaction_amount'},inplace=True)
        self.data = groupby_data
        
        return self.data
        
        
        
    def save_data(self, output_file: str) -> pd.DataFrame:
        
        """
        Saving transformed data into a new CSV output file. Prints filename when saved.
        """
        
        self.data.to_csv(output_file,index=False)
        print('Transformed CSV saved as: {}'.format(output_file))
        
        return self.data
        

        
def main():
    
    """
    Runs the full ETL pipeline.
    1) cleans the CSV and saves the data to 'cleaned_data.csv'
    2) transforms the CSV and saves the data to 'aggregated_transactions.csv'
    3) prints the final output data
    
    """
    
    clean_csv = CSVCleaner('input_data.csv') 
    
    clean_csv.read_data()    
    clean_csv.remove_rows()
    clean_csv.generate_uniqueID()
    clean_csv.check_duplicates()
    clean_csv.convert_transaction_amount()
    
    clean_csv.save_data('cleaned_data.csv')
    
    # now take cleaned csv file and apply transformations to it by putting it through transformation class
    
    transform_csv = Transformations('cleaned_data.csv')
    
    transform_csv.group_by_customer_id()
    
    transform_csv.save_data('aggregated_transactions.csv')
    
    
    # display dataframes. remove # to see all

    #input_csv = pd.read_csv('input_data.csv',delimiter=',')
    #print('\n The input CSV:\n\n {}'.format(input_csv))
    #print('\n The cleaned CSV:\n\n {}'.format(clean_csv.data))
    print('\n The final output CSV (cleaned and transformed):\n\n {}'.format(transform_csv.data))

    
    
if __name__ == "__main__":
    
    main()
