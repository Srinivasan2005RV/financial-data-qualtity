"""
Data Quality Framework Main Class
Orchestrates all validation checks and manages results with Azure SQL Database support
"""

import pandas as pd
import json
import os
from datetime import datetime
from typing import Dict, List, Any, Tuple, Optional
from src.validators import (
    validate_non_null_fields,
    validate_amount_range,
    validate_currency_codes,
    validate_duplicate_transactions,
    validate_timestamp_format,
    validate_account_id_format
)
from src.utils import load_config, save_failed_records, create_directory_if_not_exists

# Azure SQL connector import (optional)
try:
    from src.azure_sql_connector import AzureSQLConnector
    AZURE_SQL_AVAILABLE = True
except ImportError:
    AZURE_SQL_AVAILABLE = False


class DataQualityFramework:
    """
    Main class for data quality validation framework with Azure SQL Database support
    """
    
    def __init__(self, config_path: str = "config/data_quality_config.json", 
                 use_azure_sql: bool = False):
        """
        Initialize the framework with configuration
        
        Args:
            config_path: Path to configuration file
            use_azure_sql: Whether to use Azure SQL Database integration
        """
        self.config = load_config(config_path)
        self.validation_results = {}
        self.failed_records = {}
        self.summary_stats = {}
        self.use_azure_sql = use_azure_sql
        
        # Initialize Azure SQL connector if requested and available
        self.azure_connector = None
        if use_azure_sql and AZURE_SQL_AVAILABLE:
            try:
                self.azure_connector = AzureSQLConnector()
                print("✅ Azure SQL Database connector initialized")
            except Exception as e:
                print(f"⚠️ Azure SQL Database initialization failed: {e}")
                print("📝 Falling back to CSV file operations")
                self.use_azure_sql = False
        elif use_azure_sql and not AZURE_SQL_AVAILABLE:
            print("⚠️ Azure SQL Database dependencies not available")
            print("📝 Install pyodbc and related packages to use Azure SQL")
            self.use_azure_sql = False
        
        # Load approved currencies
        currencies_config = load_config("config/currencies.json")
        self.approved_currencies = currencies_config.get("approved_currencies", [])
    
    def run_all_validations(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Run all configured validation checks
        
        Args:
            df: Input DataFrame to validate
            
        Returns:
            Dictionary containing validation results
        """
        print(f"Starting data quality validation for {len(df)} records...")
        
        # Initialize results
        self.validation_results = {}
        self.failed_records = {}
        current_df = df.copy()
        
        # 1. Validate mandatory fields
        print("✓ Checking mandatory fields...")
        passed_df, failed_df = validate_non_null_fields(
            current_df, 
            self.config["validation_rules"]["mandatory_fields"]
        )
        self._store_validation_result("mandatory_fields", passed_df, failed_df, len(df))
        current_df = passed_df
        
        # 2. Validate amount range
        if len(current_df) > 0:
            print("✓ Checking amount range...")
            amount_config = self.config["validation_rules"]["amount_validation"]
            passed_df, failed_df = validate_amount_range(
                current_df, 
                amount_config["min_value"], 
                amount_config["max_value"]
            )
            self._store_validation_result("amount_range", passed_df, failed_df, len(current_df))
            current_df = passed_df
        
        # 3. Validate currency codes
        if len(current_df) > 0:
            print("✓ Checking currency codes...")
            passed_df, failed_df = validate_currency_codes(current_df, self.approved_currencies)
            self._store_validation_result("currency_codes", passed_df, failed_df, len(current_df))
            current_df = passed_df
        
        # 4. Validate duplicate transactions
        if len(current_df) > 0:
            print("✓ Checking for duplicate transactions...")
            passed_df, failed_df = validate_duplicate_transactions(current_df)
            self._store_validation_result("duplicate_transactions", passed_df, failed_df, len(current_df))
            current_df = passed_df
        
        # 5. Validate timestamp format
        if len(current_df) > 0:
            print("✓ Checking timestamp format...")
            timestamp_config = self.config["validation_rules"]["timestamp_validation"]
            passed_df, failed_df = validate_timestamp_format(
                current_df,
                timestamp_config["format"],
                timestamp_config["max_future_days"]
            )
            self._store_validation_result("timestamp_format", passed_df, failed_df, len(current_df))
            current_df = passed_df
        
        # 6. Validate account ID format (optional)
        if len(current_df) > 0:
            print("✓ Checking account ID format...")
            passed_df, failed_df = validate_account_id_format(current_df)
            self._store_validation_result("account_id_format", passed_df, failed_df, len(current_df))
            current_df = passed_df
        
        # Calculate overall summary
        self._calculate_summary_stats(len(df))
        
        print(f"✓ Validation complete! {len(current_df)} records passed all checks.")
        
        return {
            "validation_results": self.validation_results,
            "failed_records": self.failed_records,
            "summary_stats": self.summary_stats,
            "clean_data": current_df
        }
    
    def _store_validation_result(self, check_name: str, passed_df: pd.DataFrame, 
                                failed_df: pd.DataFrame, total_records: int):
        """Store validation result for a specific check"""
        self.validation_results[check_name] = {
            "total_records": total_records,
            "passed_count": len(passed_df),
            "failed_count": len(failed_df),
            "pass_rate": len(passed_df) / total_records if total_records > 0 else 0
        }
        
        if len(failed_df) > 0:
            # Add validation check name and timestamp
            failed_df['validation_check'] = check_name
            failed_df['validation_timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.failed_records[check_name] = failed_df
    
    def _calculate_summary_stats(self, total_input_records: int):
        """Calculate overall summary statistics"""
        total_failed = sum([result["failed_count"] for result in self.validation_results.values()])
        total_passed = total_input_records - total_failed
        
        self.summary_stats = {
            "total_input_records": total_input_records,
            "total_passed_records": total_passed,
            "total_failed_records": total_failed,
            "overall_pass_rate": total_passed / total_input_records if total_input_records > 0 else 0,
            "validation_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "checks_performed": list(self.validation_results.keys())
        }
        
        # Determine data quality status
        pass_rate = self.summary_stats["overall_pass_rate"]
        critical_threshold = self.config["data_quality_thresholds"]["critical_pass_rate"]
        warning_threshold = self.config["data_quality_thresholds"]["warning_pass_rate"]
        
        if pass_rate >= critical_threshold:
            self.summary_stats["quality_status"] = "EXCELLENT"
        elif pass_rate >= warning_threshold:
            self.summary_stats["quality_status"] = "WARNING"
        else:
            self.summary_stats["quality_status"] = "CRITICAL"
    
    def save_failed_records(self, output_dir: str = "data/failed_records"):
        """Save failed records to files and/or Azure SQL Database"""
        # Save to CSV files (always)
        create_directory_if_not_exists(output_dir)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        for check_name, failed_df in self.failed_records.items():
            # Save to CSV
            filename = f"failed_{check_name}_{timestamp}.csv"
            filepath = os.path.join(output_dir, filename)
            failed_df.to_csv(filepath, index=False)
            print(f"✓ Saved {len(failed_df)} failed records to {filepath}")
            
            # Save to Azure SQL Database if enabled
            if self.use_azure_sql and self.azure_connector:
                try:
                    success = self.azure_connector.save_failed_records(failed_df, check_name)
                    if success:
                        print(f"✓ Saved {len(failed_df)} failed records to Azure SQL Database")
                except Exception as e:
                    print(f"⚠️ Failed to save to Azure SQL Database: {e}")
    
    def save_quality_report(self):
        """Save quality report summary to Azure SQL Database"""
        if self.use_azure_sql and self.azure_connector and self.summary_stats:
            try:
                success = self.azure_connector.save_quality_report(self.summary_stats)
                if success:
                    print("✓ Quality report saved to Azure SQL Database")
            except Exception as e:
                print(f"⚠️ Failed to save quality report to Azure SQL: {e}")
    
    def load_data_from_azure(self, query: Optional[str] = None, limit: Optional[int] = None) -> Optional[pd.DataFrame]:
        """
        Load transaction data from Azure SQL Database
        
        Args:
            query: Custom SQL query (optional)
            limit: Limit number of records (optional)
            
        Returns:
            pd.DataFrame or None: Transaction data or None if failed
        """
        if not self.use_azure_sql or not self.azure_connector:
            print("⚠️ Azure SQL Database not configured")
            return None
        
        try:
            df = self.azure_connector.load_transactions(query=query, limit=limit)
            print(f"✅ Loaded {len(df)} transactions from Azure SQL Database")
            return df
        except Exception as e:
            print(f"❌ Failed to load data from Azure SQL Database: {e}")
            return None
    
    def get_validation_summary(self) -> pd.DataFrame:
        """Get a summary DataFrame of all validation results"""
        summary_data = []
        
        for check_name, result in self.validation_results.items():
            summary_data.append({
                "Validation_Check": check_name,
                "Total_Records": result["total_records"],
                "Passed_Count": result["passed_count"],
                "Failed_Count": result["failed_count"],
                "Pass_Rate": f"{result['pass_rate']:.2%}",
                "Status": "✓ PASS" if result["pass_rate"] >= 0.95 else "⚠ WARNING" if result["pass_rate"] >= 0.90 else "✗ FAIL"
            })
        
        return pd.DataFrame(summary_data)
    
    def print_summary(self):
        """Print a formatted summary of validation results"""
        print("\n" + "="*80)
        print("DATA QUALITY VALIDATION SUMMARY")
        print("="*80)
        
        stats = self.summary_stats
        print(f"📊 Total Input Records: {stats['total_input_records']:,}")
        print(f"✅ Passed Records: {stats['total_passed_records']:,}")
        print(f"❌ Failed Records: {stats['total_failed_records']:,}")
        print(f"📈 Overall Pass Rate: {stats['overall_pass_rate']:.2%}")
        print(f"🏆 Quality Status: {stats['quality_status']}")
        print(f"⏰ Validation Time: {stats['validation_timestamp']}")
        
        print("\n📋 Detailed Results by Check:")
        print("-" * 80)
        summary_df = self.get_validation_summary()
        print(summary_df.to_string(index=False))
        
        print("\n" + "="*80)
