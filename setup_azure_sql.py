"""
Azure SQL Database Setup and Configuration Script
Run this script to set up your Azure SQL Database for the Data Quality Framework
"""

import json
import os
from src.azure_sql_connector import AzureSQLConnector

def setup_azure_sql():
    """Interactive setup for Azure SQL Database"""
    print("🔧 Azure SQL Database Setup for Data Quality Framework")
    print("=" * 60)
    
    # Check if config file exists
    config_file = "config/azure_sql_config.json"
    
    if os.path.exists(config_file):
        print(f"📁 Configuration file found: {config_file}")
        update = input("Do you want to update the existing configuration? (y/n): ").strip().lower()
        if update != 'y':
            print("Using existing configuration...")
            test_connection()
            return
    
    print("\n📝 Please provide your Azure SQL Database details:")
    print("-" * 50)
    
    # Collect configuration details
    server = input("Azure SQL Server (e.g., your-server.database.windows.net): ").strip()
    database = input("Database name: ").strip()
    username = input("Username: ").strip()
    password = input("Password: ").strip()
    
    # Create configuration
    config = {
        "azure_sql": {
            "server": server,
            "database": database,
            "username": username,
            "password": password,
            "driver": "{ODBC Driver 17 for SQL Server}",
            "connection_timeout": 30,
            "command_timeout": 30
        },
        "tables": {
            "transactions_table": "transactions",
            "failed_records_table": "failed_transactions",
            "quality_reports_table": "data_quality_reports"
        },
        "batch_size": 1000,
        "use_azure_sql": True,
        "fallback_to_csv": True
    }
    
    # Save configuration
    os.makedirs("config", exist_ok=True)
    with open(config_file, 'w') as f:
        json.dump(config, f, indent=4)
    
    print(f"\n✅ Configuration saved to {config_file}")
    
    # Test connection
    test_connection()

def test_connection():
    """Test Azure SQL Database connection"""
    print("\n🔍 Testing Azure SQL Database connection...")
    print("-" * 40)
    
    try:
        connector = AzureSQLConnector()
        
        if connector.test_connection():
            print("✅ Connection successful!")
            
            # Create tables
            print("\n📋 Creating/verifying database tables...")
            if connector.create_tables():
                print("✅ Tables created/verified successfully!")
                
                # Display table information
                print("\n📊 Database Tables:")
                print("- transactions: Stores transaction data")
                print("- failed_transactions: Stores validation failures")
                print("- data_quality_reports: Stores report summaries")
                
                print("\n🚀 Azure SQL Database is ready for use!")
                print("\nNext steps:")
                print("1. Run: python run_quality_checks_azure.py --azure-sql")
                print("2. Or use: python run_quality_checks_azure.py --data-source azure-sql")
                
            else:
                print("❌ Failed to create tables")
                troubleshoot_tables()
        else:
            print("❌ Connection failed!")
            troubleshoot_connection()
            
    except Exception as e:
        print(f"❌ Setup failed: {e}")
        troubleshoot_general()

def troubleshoot_connection():
    """Provide troubleshooting guidance for connection issues"""
    print("\n🔧 Connection Troubleshooting:")
    print("-" * 30)
    print("1. Check server name format: your-server.database.windows.net")
    print("2. Verify database name is correct")
    print("3. Confirm username and password")
    print("4. Check Azure SQL Database firewall rules")
    print("5. Ensure your IP address is whitelisted")
    print("6. Verify ODBC Driver 17 for SQL Server is installed")

def troubleshoot_tables():
    """Provide troubleshooting guidance for table creation issues"""
    print("\n🔧 Table Creation Troubleshooting:")
    print("-" * 35)
    print("1. Check if user has CREATE TABLE permissions")
    print("2. Verify database schema permissions")
    print("3. Check if tables already exist with different names")
    print("4. Review Azure SQL Database resource limits")

def troubleshoot_general():
    """Provide general troubleshooting guidance"""
    print("\n🔧 General Troubleshooting:")
    print("-" * 25)
    print("1. Install required packages:")
    print("   pip install pyodbc sqlalchemy azure-identity")
    print("2. Check Python version compatibility")
    print("3. Verify Azure SQL Database service is running")
    print("4. Review configuration file syntax")

def show_sample_data_upload():
    """Show how to upload sample data to Azure SQL"""
    print("\n📤 Sample Data Upload:")
    print("-" * 25)
    print("To upload sample transaction data to Azure SQL Database:")
    print("1. Run the framework with CSV data first:")
    print("   python run_quality_checks.py")
    print("2. Then import the CSV data to Azure SQL:")
    print("   python -c \"from src.azure_sql_connector import AzureSQLConnector; import pandas as pd; df=pd.read_csv('data/sample_transactions.csv'); connector=AzureSQLConnector(); connector.connect(); df.to_sql('transactions', connector.connection, if_exists='append', index=False)\"")

if __name__ == "__main__":
    print("🎯 Welcome to Azure SQL Database Setup!")
    print("\nThis script will help you configure Azure SQL Database")
    print("for the Data Quality Framework.\n")
    
    choice = input("Choose an option:\n1. Setup Azure SQL Database\n2. Test existing configuration\n3. Show sample data upload\nEnter choice (1-3): ").strip()
    
    if choice == "1":
        setup_azure_sql()
    elif choice == "2":
        test_connection()
    elif choice == "3":
        show_sample_data_upload()
    else:
        print("Invalid choice. Please run the script again.")
