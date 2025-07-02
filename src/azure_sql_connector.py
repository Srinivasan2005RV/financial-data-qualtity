"""
Azure SQL Database Connector for Data Quality Framework
Handles connections, data retrieval, and storage operations with Azure SQL Database
"""

import pandas as pd
import pyodbc
import json
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime
import os

class AzureSQLConnector:
    """
    Azure SQL Database connector for the Data Quality Framework
    """
    
    def __init__(self, config_path: str = "config/azure_sql_config.json"):
        """
        Initialize Azure SQL connector with configuration
        
        Args:
            config_path: Path to Azure SQL configuration file
        """
        self.config = self._load_config(config_path)
        self.connection = None
        self.logger = logging.getLogger(__name__)
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load Azure SQL configuration from JSON file"""
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            self.logger.error(f"Configuration file not found: {config_path}")
            raise
        except json.JSONDecodeError:
            self.logger.error(f"Invalid JSON in configuration file: {config_path}")
            raise
    
    def connect(self) -> bool:
        """
        Establish connection to Azure SQL Database
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            azure_config = self.config['azure_sql']
            
            # Build connection string
            connection_string = (
                f"DRIVER={azure_config['driver']};"
                f"SERVER={azure_config['server']};"
                f"DATABASE={azure_config['database']};"
                f"UID={azure_config['username']};"
                f"PWD={azure_config['password']};"
                f"Encrypt=yes;"
                f"TrustServerCertificate=no;"
                f"Connection Timeout={azure_config['connection_timeout']};"
            )
            
            self.connection = pyodbc.connect(connection_string)
            self.logger.info("Successfully connected to Azure SQL Database")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect to Azure SQL Database: {str(e)}")
            return False
    
    def disconnect(self):
        """Close the database connection"""
        if self.connection:
            self.connection.close()
            self.logger.info("Disconnected from Azure SQL Database")
    
    def test_connection(self) -> bool:
        """
        Test the database connection
        
        Returns:
            bool: True if connection test successful
        """
        try:
            if not self.connect():
                return False
            
            # Simple test query
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1 as test")
            result = cursor.fetchone()
            cursor.close()
            
            self.logger.info("Azure SQL Database connection test successful")
            return True
            
        except Exception as e:
            self.logger.error(f"Connection test failed: {str(e)}")
            return False
        finally:
            self.disconnect()
    
    def load_transactions(self, query: Optional[str] = None, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Load transaction data from Azure SQL Database
        
        Args:
            query: Custom SQL query (optional)
            limit: Limit number of records (optional)
            
        Returns:
            pd.DataFrame: Transaction data
        """
        try:
            if not self.connect():
                raise Exception("Failed to connect to database")
            
            # Default query if none provided
            if query is None:
                table_name = self.config['tables']['transactions_table']
                query = f"SELECT transaction_id, account_id, amount, currency, timestamp FROM {table_name}"
                
                if limit:
                    query += f" TOP {limit}"
            
            # Load data using pandas
            df = pd.read_sql(query, self.connection)
            self.logger.info(f"Loaded {len(df)} transactions from Azure SQL Database")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to load transactions: {str(e)}")
            raise
        finally:
            self.disconnect()
    
    def save_failed_records(self, failed_df: pd.DataFrame, validation_type: str) -> bool:
        """
        Save failed records to Azure SQL Database
        
        Args:
            failed_df: DataFrame containing failed records
            validation_type: Type of validation that failed
            
        Returns:
            bool: True if save successful
        """
        try:
            if failed_df.empty:
                return True
                
            if not self.connect():
                return False
            
            # Add metadata columns
            failed_df = failed_df.copy()
            failed_df['validation_type'] = validation_type
            failed_df['failed_at'] = datetime.now()
            failed_df['status'] = 'FAILED'
            
            # Save to failed records table
            table_name = self.config['tables']['failed_records_table']
            
            # Use to_sql with pyodbc connection
            failed_df.to_sql(
                name=table_name,
                con=self.connection,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=self.config['batch_size']
            )
            
            self.logger.info(f"Saved {len(failed_df)} failed records to Azure SQL Database")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to save failed records: {str(e)}")
            return False
        finally:
            self.disconnect()
    
    def save_quality_report(self, report_data: Dict[str, Any]) -> bool:
        """
        Save data quality report summary to Azure SQL Database
        
        Args:
            report_data: Dictionary containing report metrics
            
        Returns:
            bool: True if save successful
        """
        try:
            if not self.connect():
                return False
            
            # Prepare report data
            report_df = pd.DataFrame([{
                'report_date': datetime.now(),
                'total_records': report_data.get('total_records', 0),
                'passed_records': report_data.get('passed_records', 0),
                'failed_records': report_data.get('failed_records', 0),
                'pass_rate': report_data.get('pass_rate', 0.0),
                'quality_status': report_data.get('quality_status', 'UNKNOWN'),
                'validation_details': json.dumps(report_data.get('validation_details', {}))
            }])
            
            # Save to quality reports table
            table_name = self.config['tables']['quality_reports_table']
            
            report_df.to_sql(
                name=table_name,
                con=self.connection,
                if_exists='append',
                index=False
            )
            
            self.logger.info("Saved quality report to Azure SQL Database")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to save quality report: {str(e)}")
            return False
        finally:
            self.disconnect()
    
    def create_tables(self) -> bool:
        """
        Create necessary tables in Azure SQL Database if they don't exist
        
        Returns:
            bool: True if tables created successfully
        """
        try:
            if not self.connect():
                return False
            
            cursor = self.connection.cursor()
            
            # Create transactions table (if needed)
            transactions_table = self.config['tables']['transactions_table']
            create_transactions_sql = f"""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{transactions_table}' AND xtype='U')
            CREATE TABLE {transactions_table} (
                transaction_id NVARCHAR(50) PRIMARY KEY,
                account_id NVARCHAR(50) NOT NULL,
                amount DECIMAL(18,2) NOT NULL,
                currency NVARCHAR(3) NOT NULL,
                timestamp DATETIME NOT NULL,
                created_at DATETIME DEFAULT GETDATE()
            )
            """
            
            # Create failed records table
            failed_table = self.config['tables']['failed_records_table']
            create_failed_sql = f"""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{failed_table}' AND xtype='U')
            CREATE TABLE {failed_table} (
                id INT IDENTITY(1,1) PRIMARY KEY,
                transaction_id NVARCHAR(50),
                account_id NVARCHAR(50),
                amount DECIMAL(18,2),
                currency NVARCHAR(3),
                timestamp DATETIME,
                validation_type NVARCHAR(50),
                failed_at DATETIME,
                status NVARCHAR(20),
                created_at DATETIME DEFAULT GETDATE()
            )
            """
            
            # Create quality reports table
            reports_table = self.config['tables']['quality_reports_table']
            create_reports_sql = f"""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{reports_table}' AND xtype='U')
            CREATE TABLE {reports_table} (
                id INT IDENTITY(1,1) PRIMARY KEY,
                report_date DATETIME NOT NULL,
                total_records INT NOT NULL,
                passed_records INT NOT NULL,
                failed_records INT NOT NULL,
                pass_rate FLOAT NOT NULL,
                quality_status NVARCHAR(20),
                validation_details NVARCHAR(MAX),
                created_at DATETIME DEFAULT GETDATE()
            )
            """
            
            # Execute table creation
            cursor.execute(create_transactions_sql)
            cursor.execute(create_failed_sql)
            cursor.execute(create_reports_sql)
            
            self.connection.commit()
            cursor.close()
            
            self.logger.info("Successfully created/verified Azure SQL Database tables")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create tables: {str(e)}")
            return False
        finally:
            self.disconnect()

# Example usage and configuration
if __name__ == "__main__":
    # Test the Azure SQL connector
    connector = AzureSQLConnector()
    
    if connector.test_connection():
        print("✅ Azure SQL Database connection successful!")
        
        # Create tables
        if connector.create_tables():
            print("✅ Database tables created/verified successfully!")
        else:
            print("❌ Failed to create database tables")
    else:
        print("❌ Azure SQL Database connection failed!")
        print("Please check your configuration in config/azure_sql_config.json")
