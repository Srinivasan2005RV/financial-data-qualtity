"""
Unit tests for data quality validators
"""

import unittest
import pandas as pd
import numpy as np
import sys
import os

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from validators import (
    validate_non_null_fields,
    validate_amount_range,
    validate_currency_codes,
    validate_duplicate_transactions,
    validate_timestamp_format,
    validate_account_id_format
)


class TestDataQualityValidators(unittest.TestCase):
    """Test cases for data quality validators"""
    
    def setUp(self):
        """Set up test data"""
        self.sample_data = pd.DataFrame({
            'transaction_id': ['TXN001', 'TXN002', 'TXN003', 'TXN004', 'TXN005'],
            'account_id': ['ACC123', 'ACC456', None, 'ACC789', 'ACC123'],
            'amount': [100.50, -50.00, 200.75, 0.00, 1500000.00],
            'currency': ['USD', 'EUR', 'XXX', 'GBP', 'USD'],
            'timestamp': ['2025-01-01 10:00:00', '2025-01-02 11:00:00', 
                         'invalid-date', '2025-01-03 12:00:00', '2025-01-04 13:00:00']
        })
        
        self.approved_currencies = ['USD', 'EUR', 'GBP', 'JPY', 'CAD']
    
    def test_validate_non_null_fields(self):
        """Test non-null field validation"""
        mandatory_fields = ['transaction_id', 'account_id', 'amount']
        
        passed_df, failed_df = validate_non_null_fields(self.sample_data, mandatory_fields)
        
        # Should have 1 failed record (row with null account_id)
        self.assertEqual(len(failed_df), 1)
        self.assertEqual(len(passed_df), 4)
        self.assertTrue(failed_df.iloc[0]['account_id'] is None or pd.isna(failed_df.iloc[0]['account_id']))
    
    def test_validate_amount_range(self):
        """Test amount range validation"""
        min_amount = 0.01
        max_amount = 1000000.00
        
        passed_df, failed_df = validate_amount_range(self.sample_data, min_amount, max_amount)
        
        # Should have 3 failed records (negative, zero, and too large amounts)
        self.assertEqual(len(failed_df), 3)
        self.assertEqual(len(passed_df), 2)
        
        # Check specific failures
        failed_amounts = failed_df['amount'].tolist()
        self.assertIn(-50.00, failed_amounts)  # Negative amount
        self.assertIn(0.00, failed_amounts)    # Zero amount
        self.assertIn(1500000.00, failed_amounts)  # Amount too large
    
    def test_validate_currency_codes(self):
        """Test currency code validation"""
        passed_df, failed_df = validate_currency_codes(self.sample_data, self.approved_currencies)
        
        # Should have 1 failed record (XXX currency)
        self.assertEqual(len(failed_df), 1)
        self.assertEqual(len(passed_df), 4)
        self.assertEqual(failed_df.iloc[0]['currency'], 'XXX')
    
    def test_validate_duplicate_transactions(self):
        """Test duplicate transaction validation"""
        # Create data with duplicates
        duplicate_data = self.sample_data.copy()
        duplicate_data.loc[4, 'transaction_id'] = 'TXN001'  # Create duplicate
        
        passed_df, failed_df = validate_duplicate_transactions(duplicate_data)
        
        # Should have 2 failed records (both instances of the duplicate)
        self.assertEqual(len(failed_df), 2)
        self.assertEqual(len(passed_df), 3)
    
    def test_validate_timestamp_format(self):
        """Test timestamp format validation"""
        passed_df, failed_df = validate_timestamp_format(self.sample_data)
        
        # Should have 1 failed record (invalid-date)
        self.assertEqual(len(failed_df), 1)
        self.assertEqual(len(passed_df), 4)
        self.assertIn('invalid-date', failed_df['timestamp'].tolist())
    
    def test_validate_account_id_format(self):
        """Test account ID format validation"""
        passed_df, failed_df = validate_account_id_format(self.sample_data)
        
        # Should have 1 failed record (null account_id)
        self.assertEqual(len(failed_df), 1)
        self.assertEqual(len(passed_df), 4)
    
    def test_empty_dataframe(self):
        """Test validators with empty DataFrame"""
        empty_df = pd.DataFrame(columns=['transaction_id', 'account_id', 'amount', 'currency', 'timestamp'])
        
        passed_df, failed_df = validate_non_null_fields(empty_df, ['transaction_id'])
        
        self.assertEqual(len(passed_df), 0)
        self.assertEqual(len(failed_df), 0)
    
    def test_all_valid_data(self):
        """Test validators with completely valid data"""
        valid_data = pd.DataFrame({
            'transaction_id': ['TXN001', 'TXN002', 'TXN003'],
            'account_id': ['ACC123', 'ACC456', 'ACC789'],
            'amount': [100.50, 50.25, 200.75],
            'currency': ['USD', 'EUR', 'GBP'],
            'timestamp': ['2025-01-01 10:00:00', '2025-01-02 11:00:00', '2025-01-03 12:00:00']
        })
        
        # Test all validators
        passed_df, failed_df = validate_non_null_fields(valid_data, ['transaction_id', 'account_id'])
        self.assertEqual(len(failed_df), 0)
        self.assertEqual(len(passed_df), 3)
        
        passed_df, failed_df = validate_amount_range(valid_data)
        self.assertEqual(len(failed_df), 0)
        self.assertEqual(len(passed_df), 3)
        
        passed_df, failed_df = validate_currency_codes(valid_data, self.approved_currencies)
        self.assertEqual(len(failed_df), 0)
        self.assertEqual(len(passed_df), 3)
        
        passed_df, failed_df = validate_duplicate_transactions(valid_data)
        self.assertEqual(len(failed_df), 0)
        self.assertEqual(len(passed_df), 3)
        
        passed_df, failed_df = validate_timestamp_format(valid_data)
        self.assertEqual(len(failed_df), 0)
        self.assertEqual(len(passed_df), 3)


class TestEdgeCases(unittest.TestCase):
    """Test edge cases and error conditions"""
    
    def test_missing_columns(self):
        """Test behavior with missing columns"""
        incomplete_data = pd.DataFrame({
            'transaction_id': ['TXN001', 'TXN002'],
            'amount': [100.50, 200.75]
            # Missing account_id, currency, timestamp
        })
        
        # This should handle missing columns gracefully
        with self.assertRaises(KeyError):
            validate_non_null_fields(incomplete_data, ['transaction_id', 'account_id'])
    
    def test_extreme_amounts(self):
        """Test with extreme amount values"""
        extreme_data = pd.DataFrame({
            'transaction_id': ['TXN001', 'TXN002', 'TXN003'],
            'account_id': ['ACC123', 'ACC456', 'ACC789'],
            'amount': [0.001, 999999999999.99, float('inf')],
            'currency': ['USD', 'EUR', 'GBP'],
            'timestamp': ['2025-01-01 10:00:00', '2025-01-02 11:00:00', '2025-01-03 12:00:00']
        })
        
        passed_df, failed_df = validate_amount_range(extreme_data, 0.01, 1000000.00)
        
        # Very small amount and infinite amount should fail
        self.assertGreaterEqual(len(failed_df), 2)
    
    def test_special_characters_in_currency(self):
        """Test currency validation with special characters"""
        special_data = pd.DataFrame({
            'transaction_id': ['TXN001', 'TXN002', 'TXN003'],
            'account_id': ['ACC123', 'ACC456', 'ACC789'],
            'amount': [100.50, 200.75, 300.25],
            'currency': ['USD', '€UR', 'GB£'],
            'timestamp': ['2025-01-01 10:00:00', '2025-01-02 11:00:00', '2025-01-03 12:00:00']
        })
        
        approved_currencies = ['USD', 'EUR', 'GBP']
        passed_df, failed_df = validate_currency_codes(special_data, approved_currencies)
        
        # Special character currencies should fail
        self.assertEqual(len(failed_df), 2)
        self.assertEqual(len(passed_df), 1)


if __name__ == '__main__':
    # Create test suite
    test_suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # Print summary
    print(f"\n{'='*50}")
    print(f"TESTS SUMMARY")
    print(f"{'='*50}")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    
    if result.wasSuccessful():
        print("✅ All tests passed!")
    else:
        print("❌ Some tests failed.")
        
        if result.failures:
            print("\nFailures:")
            for test, failure in result.failures:
                print(f"  - {test}: {failure}")
        
        if result.errors:
            print("\nErrors:")
            for test, error in result.errors:
                print(f"  - {test}: {error}")
