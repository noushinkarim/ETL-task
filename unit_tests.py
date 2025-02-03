import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import unittest
from hashlib import md5
from io import StringIO

# importing classes to be tested from original script
from ETL_Exercise import CSVCleaner, Transformations


class TestCSVCleaner(unittest.TestCase):
    """
    Unit tests on the CSVCleaner class methods.
    Creates a test DataFrame based on input CSV but more rows and NaNs.
    After setting up the test, implement a tearDown method to remove the test
    file after each test, which prevents cluttering of the directory that may
    cause failures in future tests. Each method is tested, with assertions
    based on the expected output.
    """

    def setUp(self):
        """
        Sets up the test DataFrame and saves to a CSV file.
        """
        self.test_data = pd.DataFrame({
            'transaction_id': [
                1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010, 1011, 1012
            ],
            'customer_id': [
                'ABC123', 'XYZ789', 'ABC123', 'XYZ789', 'LMN456', 'ABC123',
                'ABC123', 'DEF567', 'XYZ789', 'LMN456', 'ABC123', 'NULL'
            ],
            'transaction_amount': [
                45.67, np.nan, 20, 35.5, 50, 15.5, 20, 75.25, 40, np.nan, np.nan, 100.5
            ],
            'date': [
                '10/01/2024', '11/01/2024', '12/01/2024', '13/01/2024',
                '14/01/2024', '15/01/2024', '12/01/2024', '16/01/2024',
                '17/01/2024', '18/01/2024', '19/01/2024', '20/01/2024'
            ]
        })
        self.test_csv_path = 'test_input_data.csv'
        self.test_data.to_csv(self.test_csv_path, index=False)

    def tearDown(self):
        """
        This removes the test CSV file after each test, to avoid cluttering of
        directory that could cause problems for future tests.
        """
        if os.path.exists(self.test_csv_path):
            os.remove(self.test_csv_path)

    def test_read_data(self):
        """
        This is a basic test that creates an instance (object) of the CSVCleaner
        class, and then its methods can be used, namely here: read_data() from
        CSVCleaner. Checks if it's a dataframe and then sets the assertion, which
        compares the len(of the data) with the expected output.
        """
        # initialise
        cleaner = CSVCleaner(self.test_csv_path)
        # implement method to test
        data = cleaner.read_data()
        # set assertions
        self.assertIsInstance(data, pd.DataFrame)
        self.assertEqual(len(data), 12)

    def test_remove_rows(self):
        """
        Testing CSVCleaner's remove_rows() method. Begins with initialising and
        then implementing all previous methods of the class. Sets the assertion,
        which compares the len(of the data) with the expected output. We started
        with 12 rows and should end up with 8, since 4 rows have NaNs (or a NULL).
        """
        # initialise
        cleaner = CSVCleaner(self.test_csv_path)
        # implement previous methods
        cleaner.read_data()
        # implement method to test
        cleaned_data = cleaner.remove_rows()
        # set assertions
        self.assertEqual(len(cleaned_data), 8)

    def test_generate_uniqueID(self):
        """
        Testing CSVCleaner's generate_uniqueID() method. Begins with initialising
        and then implementing all previous methods of the class. Sets the
        assertion, and checks that each generated ID is actually unique.
        """
        # initialise
        cleaner = CSVCleaner(self.test_csv_path)
        # implement previous methods
        cleaner.read_data()
        cleaner.remove_rows()
        # implement method to test
        data_with_id = cleaner.generate_uniqueID()
        # set assertions
        self.assertIn('unique_id', data_with_id.columns)
        self.assertEqual(len(data_with_id), 8)
        self.assertTrue(data_with_id['unique_id'].is_unique)

    def test_check_duplicates(self):
        """
        Testing CSVCleaner's check_duplicates() method. Begins with initialising
        and then implementing all previous methods of the class. Sets the
        assertion, and checks that there are no duplicates, and the expected len().
        """
        # initialise
        cleaner = CSVCleaner(self.test_csv_path)
        # implement previous methods
        cleaner.read_data()
        cleaner.remove_rows()
        cleaner.generate_uniqueID()
        # implement method to test
        data_no_duplicates = cleaner.check_duplicates()
        # set assertion
        self.assertEqual(len(data_no_duplicates), 8)

    def test_convert_transaction_amount(self):
        """
        Testing CSVCleaner's convert_transaction_amount() method. Begins with
        initialising and then implementing relevant previous methods of the class,
        i.e. this does not depend on generating unique IDs and checking for
        duplicates, so do not need to include those methods here. Sets the
        assertion, and checks that all items are float data types.
        """
        # initialise
        cleaner = CSVCleaner(self.test_csv_path)
        # implement previous methods that are relevant/dependent
        cleaner.read_data()
        cleaner.remove_rows()
        # implement method to test
        data_converted = cleaner.convert_transaction_amount()
        # set assertions
        self.assertTrue(
            pd.api.types.is_float_dtype(data_converted['transaction_amount'])
        )

    def test_save_data(self):
        """
        Testing CSVCleaner's save_data() method. Begins with initialising and then
        implementing all previous methods of the class. Saves the output data,
        reads it in again, compares it to the stored test data. Checks both
        dataframes are the same.
        """
        # initialise
        cleaner = CSVCleaner(self.test_csv_path)
        # implement all previous methods
        cleaner.read_data()
        cleaner.remove_rows()
        cleaner.generate_uniqueID()
        cleaner.check_duplicates()
        cleaner.convert_transaction_amount()
        # name output file
        output_file = 'test_cleaned_data.csv'
        # implement method to test
        cleaner.save_data(output_file)
        # reading in saved CSV in standard way
        saved_data = pd.read_csv(output_file)
        # reset the index of test cleaner.data to match auto-reset saved data
        cleaner.data.reset_index(drop=True, inplace=True)
        # set assertions
        pd.testing.assert_frame_equal(saved_data, cleaner.data)
        # remove the output file to keep things tidy in directory
        os.remove(output_file)


class TestTransformations(unittest.TestCase):
    """
    This is a unit test on the Transformations method. Inputs the cleaned test DF
    with NaNs and nulls removed. After setting up the test, implement a tearDown
    to remove the test file after each test, which prevents cluttering of the
    directory that may cause failures in future tests. Each method is tested,
    with assertions put in based on the expected output.
    """

    def setUp(self):
        """
        This puts in the cleaned_data with NaNs and nulls removed and saves it to
        a CSV.
        """
        self.cleaned_data = pd.DataFrame({
            'transaction_id': [
                1001, 1003, 1004, 1005, 1006, 1007, 1008, 1009
            ],
            'customer_id': [
                'ABC123', 'ABC123', 'XYZ789', 'LMN456', 'ABC123', 'ABC123',
                'DEF567', 'XYZ789'
            ],
            'transaction_amount': [
                45.67, 20, 35.5, 50, 15.5, 20, 75.25, 40
            ],
            'date': [
                '10/01/2024', '12/01/2024', '13/01/2024', '14/01/2024',
                '15/01/2024', '12/01/2024', '16/01/2024', '17/01/2024'
            ]
        })
        self.cleaned_csv_path = 'test_cleaned_data.csv'
        self.cleaned_data.to_csv(self.cleaned_csv_path, index=False)

    def tearDown(self):
        """
        This removes the cleaned dataset after each test to avoid clutter in the
        directory, as well as avoiding problems with future tests.
        """
        if os.path.exists(self.cleaned_csv_path):
            os.remove(self.cleaned_csv_path)

    def test_group_by_customer_id(self):
        """
        This tests the Transformations class group_by_customer_id() method. An
        instance of the Transformations class is created, then the group by method
        is executed and the resulting dataframe is compared to a specified
        expected output dataframe.
        """
        # initialise
        transformer = Transformations(self.cleaned_csv_path)
        # implement method to test
        grouped_data = transformer.group_by_customer_id()
        # specify expected dataframe after group by method
        expected_data = pd.DataFrame({
            'customer_id': ['ABC123', 'DEF567', 'LMN456', 'XYZ789'],
            'total_transaction_amount': [101.17, 75.25, 50.0, 75.5]
        })
        # set assertions that the two dataframes are equal
        pd.testing.assert_frame_equal(grouped_data, expected_data)

    def test_save_data(self):
        """
        Testing the save_data() method, as in previous unit test class.
        """
        # initialise
        transformer = Transformations(self.cleaned_csv_path)
        # implement previous method
        transformer.group_by_customer_id()
        # name output
        output_file = 'test_aggregated_transactions.csv'
        # implement method to test
        transformer.save_data(output_file)
        # reading in saved CSV in standard way
        saved_data = pd.read_csv(output_file)
        # reset the index of test cleaner.data to match auto-reset saved data
        transformer.data.reset_index(drop=True, inplace=True)
        # set assertions that both dataframes are equal
        pd.testing.assert_frame_equal(saved_data, transformer.data)
        # remove the output file to keep things tidy in directory
        os.remove(output_file)


if __name__ == '__main__':
    unittest.main()