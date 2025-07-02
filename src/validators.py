"""
Data Quality Framework for Financial Transactions
Core validation functions for data quality checks
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any
import json
import os


def validate_non_null_fields(df: pd.DataFrame, mandatory_fields: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validate that mandatory fields are not null
    
    Args:
        df: Input DataFrame
        mandatory_fields: List of field names that cannot be null
        
    Returns:
        Tuple of (passed_records, failed_records)
    """
    mask = df[mandatory_fields].isnull().any(axis=1)
    failed_records = df[mask].copy()
    
    if len(failed_records) > 0:
        failed_records['failure_reason'] = 'Missing mandatory field(s)'
        failed_records['failed_fields'] = df[mask][mandatory_fields].isnull().apply(
            lambda x: ', '.join(x.index[x]), axis=1
        )
    else:
        # Add columns for consistency even when no failures
        failed_records = failed_records.reindex(columns=list(failed_records.columns) + ['failure_reason', 'failed_fields'])
    
    passed_records = df[~mask].copy()
    
    return passed_records, failed_records


def validate_amount_range(df: pd.DataFrame, min_value: float = 0.01, max_value: float = 1000000.00) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validate that amount is within acceptable range
    
    Args:
        df: Input DataFrame
        min_value: Minimum allowed amount
        max_value: Maximum allowed amount
        
    Returns:
        Tuple of (passed_records, failed_records)
    """
    mask = (df['amount'] <= 0) | (df['amount'] > max_value) | df['amount'].isnull()
    failed_records = df[mask].copy()
    
    if len(failed_records) > 0:
        failed_records['failure_reason'] = f'Amount not in valid range ({min_value} - {max_value})'
        failed_records['failed_fields'] = 'amount'
    else:
        # Add columns for consistency even when no failures
        failed_records = failed_records.reindex(columns=list(failed_records.columns) + ['failure_reason', 'failed_fields'])
    
    passed_records = df[~mask].copy()
    
    return passed_records, failed_records


def validate_currency_codes(df: pd.DataFrame, approved_currencies: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validate that currency codes are in approved list
    
    Args:
        df: Input DataFrame
        approved_currencies: List of approved currency codes
        
    Returns:
        Tuple of (passed_records, failed_records)
    """
    mask = ~df['currency'].isin(approved_currencies) | df['currency'].isnull()
    failed_records = df[mask].copy()
    failed_records['failure_reason'] = f'Currency not in approved list: {approved_currencies}'
    failed_records['failed_fields'] = 'currency'
    
    passed_records = df[~mask].copy()
    
    return passed_records, failed_records


def validate_duplicate_transactions(df: pd.DataFrame, id_column: str = 'transaction_id') -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validate that there are no duplicate transaction IDs
    
    Args:
        df: Input DataFrame
        id_column: Column name for transaction ID
        
    Returns:
        Tuple of (passed_records, failed_records)
    """
    duplicates_mask = df.duplicated(subset=[id_column], keep=False)
    failed_records = df[duplicates_mask].copy()
    failed_records['failure_reason'] = 'Duplicate transaction ID'
    failed_records['failed_fields'] = id_column
    
    passed_records = df[~duplicates_mask].copy()
    
    return passed_records, failed_records


def validate_timestamp_format(df: pd.DataFrame, timestamp_format: str = "%Y-%m-%d %H:%M:%S", 
                             max_future_days: int = 1) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validate timestamp format and reasonable date range
    
    Args:
        df: Input DataFrame
        timestamp_format: Expected timestamp format
        max_future_days: Maximum days in the future allowed
        
    Returns:
        Tuple of (passed_records, failed_records)
    """
    failed_records_list = []
    
    # Check for null timestamps
    null_mask = df['timestamp'].isnull()
    if null_mask.any():
        null_failed = df[null_mask].copy()
        null_failed['failure_reason'] = 'Null timestamp'
        null_failed['failed_fields'] = 'timestamp'
        failed_records_list.append(null_failed)
    
    # Work with non-null timestamps
    non_null_df = df[~null_mask].copy()
    
    if len(non_null_df) > 0:
        try:
            # Try to parse timestamps
            parsed_timestamps = pd.to_datetime(non_null_df['timestamp'], format=timestamp_format, errors='coerce')
            
            # Check for parsing failures
            parse_fail_mask = parsed_timestamps.isnull()
            if parse_fail_mask.any():
                parse_failed = non_null_df[parse_fail_mask].copy()
                parse_failed['failure_reason'] = f'Invalid timestamp format (expected: {timestamp_format})'
                parse_failed['failed_fields'] = 'timestamp'
                failed_records_list.append(parse_failed)
            
            # Check for future dates beyond threshold
            valid_parsed = non_null_df[~parse_fail_mask].copy()
            if len(valid_parsed) > 0:
                valid_timestamps = parsed_timestamps[~parse_fail_mask]
                future_threshold = datetime.now() + timedelta(days=max_future_days)
                future_mask = valid_timestamps > future_threshold
                
                if future_mask.any():
                    future_failed = valid_parsed[future_mask].copy()
                    future_failed['failure_reason'] = f'Timestamp too far in future (max {max_future_days} days)'
                    future_failed['failed_fields'] = 'timestamp'
                    failed_records_list.append(future_failed)
                
                passed_records = valid_parsed[~future_mask].copy()
            else:
                passed_records = pd.DataFrame(columns=df.columns)
        except Exception as e:
            # If parsing completely fails, mark all as failed
            all_failed = non_null_df.copy()
            all_failed['failure_reason'] = f'Timestamp parsing error: {str(e)}'
            all_failed['failed_fields'] = 'timestamp'
            failed_records_list.append(all_failed)
            passed_records = pd.DataFrame(columns=df.columns)
    else:
        passed_records = pd.DataFrame(columns=df.columns)
    
    # Combine all failed records
    if failed_records_list:
        failed_records = pd.concat(failed_records_list, ignore_index=True)
    else:
        failed_records = pd.DataFrame(columns=df.columns.tolist() + ['failure_reason', 'failed_fields'])
    
    return passed_records, failed_records


def validate_account_id_format(df: pd.DataFrame, pattern: str = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validate account ID format (optional validation)
    
    Args:
        df: Input DataFrame
        pattern: Regex pattern for account ID validation
        
    Returns:
        Tuple of (passed_records, failed_records)
    """
    if pattern is None:
        # Basic validation - just check for non-empty strings
        mask = df['account_id'].astype(str).str.strip().eq('') | df['account_id'].isnull()
    else:
        import re
        mask = ~df['account_id'].astype(str).str.match(pattern) | df['account_id'].isnull()
    
    failed_records = df[mask].copy()
    failed_records['failure_reason'] = 'Invalid account ID format'
    failed_records['failed_fields'] = 'account_id'
    
    passed_records = df[~mask].copy()
    
    return passed_records, failed_records
