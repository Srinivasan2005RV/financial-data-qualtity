"""
Utility functions for the data quality framework
"""

import json
import os
import pandas as pd
from datetime import datetime
from typing import Dict, Any


def load_config(config_path: str) -> Dict[str, Any]:
    """
    Load configuration from JSON file
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        Configuration dictionary
    """
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in configuration file {config_path}: {e}")


def create_directory_if_not_exists(directory_path: str):
    """
    Create directory if it doesn't exist
    
    Args:
        directory_path: Path to directory to create
    """
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"✓ Created directory: {directory_path}")


def save_failed_records(failed_records_dict: Dict[str, pd.DataFrame], 
                       output_dir: str = "data/failed_records"):
    """
    Save failed records to CSV files
    
    Args:
        failed_records_dict: Dictionary of failed records by validation check
        output_dir: Output directory for failed records
    """
    create_directory_if_not_exists(output_dir)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    for check_name, failed_df in failed_records_dict.items():
        if len(failed_df) > 0:
            filename = f"failed_{check_name}_{timestamp}.csv"
            filepath = os.path.join(output_dir, filename)
            failed_df.to_csv(filepath, index=False)
            print(f"✓ Saved {len(failed_df)} failed records to {filepath}")


def generate_sample_data(num_records: int = 1000, include_errors: bool = True) -> pd.DataFrame:
    """
    Generate sample transaction data for testing
    
    Args:
        num_records: Number of records to generate
        include_errors: Whether to include some erroneous records for testing
        
    Returns:
        DataFrame with sample transaction data
    """
    import random
    import numpy as np
    from datetime import timedelta
    
    # Base data generation
    transaction_ids = [f"TXN{str(i).zfill(8)}" for i in range(1, num_records + 1)]
    account_ids = [f"ACC{random.randint(100000, 999999)}" for _ in range(num_records)]
    amounts = np.random.lognormal(mean=3, sigma=1, size=num_records).round(2)
    
    # Valid currencies (most records)
    valid_currencies = ["USD", "EUR", "GBP", "JPY", "CAD"]
    currencies = [random.choice(valid_currencies) for _ in range(num_records)]
    
    # Generate timestamps
    base_time = datetime.now() - timedelta(days=30)
    timestamps = []
    for i in range(num_records):
        random_offset = random.randint(0, 30 * 24 * 60 * 60)  # Up to 30 days
        timestamp = base_time + timedelta(seconds=random_offset)
        timestamps.append(timestamp.strftime("%Y-%m-%d %H:%M:%S"))
    
    # Create DataFrame
    df = pd.DataFrame({
        'transaction_id': transaction_ids,
        'account_id': account_ids,
        'amount': amounts,
        'currency': currencies,
        'timestamp': timestamps
    })
    
    if include_errors:
        # Introduce some errors for testing
        error_indices = random.sample(range(num_records), min(50, num_records // 20))
        
        for idx in error_indices:
            error_type = random.choice(['null_field', 'negative_amount', 'invalid_currency', 'duplicate_id', 'invalid_timestamp'])
            
            if error_type == 'null_field':
                field = random.choice(['transaction_id', 'account_id', 'amount', 'currency'])
                df.loc[idx, field] = None
            elif error_type == 'negative_amount':
                df.loc[idx, 'amount'] = -abs(df.loc[idx, 'amount'])
            elif error_type == 'invalid_currency':
                df.loc[idx, 'currency'] = 'XXX'
            elif error_type == 'duplicate_id' and idx > 0:
                df.loc[idx, 'transaction_id'] = df.loc[idx-1, 'transaction_id']
            elif error_type == 'invalid_timestamp':
                df.loc[idx, 'timestamp'] = 'invalid-date'
    
    return df


def format_currency(amount: float, currency: str) -> str:
    """
    Format amount with currency symbol
    
    Args:
        amount: Numeric amount
        currency: Currency code
        
    Returns:
        Formatted currency string
    """
    currency_symbols = {
        'USD': '$', 'EUR': '€', 'GBP': '£', 'JPY': '¥',
        'CAD': 'C$', 'AUD': 'A$', 'CHF': 'CHF'
    }
    
    symbol = currency_symbols.get(currency, currency)
    return f"{symbol}{amount:,.2f}"


def calculate_data_quality_score(validation_results: Dict[str, Dict]) -> float:
    """
    Calculate overall data quality score based on validation results
    
    Args:
        validation_results: Dictionary of validation results
        
    Returns:
        Data quality score (0-100)
    """
    if not validation_results:
        return 0.0
    
    # Weight different checks (some are more critical than others)
    weights = {
        'mandatory_fields': 0.3,
        'amount_range': 0.2,
        'currency_codes': 0.15,
        'duplicate_transactions': 0.2,
        'timestamp_format': 0.1,
        'account_id_format': 0.05
    }
    
    weighted_score = 0.0
    total_weight = 0.0
    
    for check_name, result in validation_results.items():
        weight = weights.get(check_name, 0.1)  # Default weight for unknown checks
        pass_rate = result.get('pass_rate', 0.0)
        weighted_score += pass_rate * weight
        total_weight += weight
    
    if total_weight > 0:
        return (weighted_score / total_weight) * 100
    else:
        return 0.0


def export_to_excel(data_dict: Dict[str, pd.DataFrame], filename: str):
    """
    Export multiple DataFrames to different sheets in an Excel file
    
    Args:
        data_dict: Dictionary where keys are sheet names and values are DataFrames
        filename: Output Excel filename
    """
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        for sheet_name, df in data_dict.items():
            # Clean sheet name (Excel has restrictions)
            clean_sheet_name = sheet_name.replace('/', '_').replace('\\', '_')[:31]
            df.to_excel(writer, sheet_name=clean_sheet_name, index=False)
    
    print(f"✓ Exported data to Excel file: {filename}")


def get_file_size_mb(filepath: str) -> float:
    """
    Get file size in megabytes
    
    Args:
        filepath: Path to file
        
    Returns:
        File size in MB
    """
    if os.path.exists(filepath):
        size_bytes = os.path.getsize(filepath)
        return size_bytes / (1024 * 1024)
    return 0.0
