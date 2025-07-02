"""
Enhanced Data Quality Framework Main Script with Azure SQL Database Support
Run this script to execute the complete data quality validation process
"""

import pandas as pd
import argparse
import os
import sys
from datetime import datetime
from src.data_quality_framework import DataQualityFramework
from src.report_generator import DataQualityReportGenerator
from src.utils import generate_sample_data

def main():
    """Main execution function with command line arguments"""
    parser = argparse.ArgumentParser(description='Data Quality Framework for Financial Data')
    parser.add_argument('--azure-sql', action='store_true', 
                       help='Use Azure SQL Database for data operations')
    parser.add_argument('--data-source', type=str, default='csv',
                       choices=['csv', 'azure-sql'],
                       help='Data source type (csv or azure-sql)')
    parser.add_argument('--limit', type=int, default=None,
                       help='Limit number of records to process')
    parser.add_argument('--config-azure', action='store_true',
                       help='Test Azure SQL Database configuration')
    
    args = parser.parse_args()
    
    print("🚀 Starting Data Quality Framework for Financial Data")
    print("=" * 60)
    
    # Test Azure SQL configuration if requested
    if args.config_azure:
        test_azure_configuration()
        return
    
    # Initialize framework
    try:
        framework = DataQualityFramework(use_azure_sql=args.azure_sql)
    except Exception as e:
        print(f"❌ Failed to initialize framework: {e}")
        return
    
    # Load data based on source type
    if args.data_source == 'azure-sql' or args.azure_sql:
        print("📡 Loading data from Azure SQL Database...")
        df = framework.load_data_from_azure(limit=args.limit)
        
        if df is None:
            print("⚠️ Failed to load from Azure SQL, falling back to CSV...")
            df = load_csv_data()
    else:
        print("📁 Loading data from CSV file...")
        df = load_csv_data()
    
    if df is None or df.empty:
        print("❌ No data available for validation")
        return
    
    print(f"✓ Loaded {len(df)} transactions for validation")
    
    # Run validation
    print("\n🔍 Running Data Quality Validations...")
    print("-" * 40)
    
    results = framework.run_all_validations(df)
    
    # Display results
    display_results(framework, results)
    
    # Save failed records
    print("\n💾 Saving failed records for inspection...")
    framework.save_failed_records()
    
    # Save quality report to Azure SQL if enabled
    if args.azure_sql:
        framework.save_quality_report()
    
    # Generate reports
    print("\n📋 Generating Data Quality Reports...")
    print("-" * 40)
    
    try:
        report_generator = DataQualityReportGenerator()
        
        # Generate reports
        report_files = report_generator.generate_weekly_report(
            validation_summary=framework.get_validation_summary(),
            overall_stats=framework.summary_stats,
            failed_records=framework.failed_records
        )
        
        print("✓ Reports generated successfully!")
        
        # Display key metrics
        display_key_metrics(framework, report_files)
        
    except Exception as e:
        print(f"⚠️ Report generation failed: {e}")
    
    print("\n✅ Data Quality Framework execution completed!")

def test_azure_configuration():
    """Test Azure SQL Database configuration"""
    print("\n🔧 Testing Azure SQL Database Configuration...")
    print("-" * 50)
    
    try:
        from src.azure_sql_connector import AzureSQLConnector
        
        connector = AzureSQLConnector()
        
        # Test connection
        if connector.test_connection():
            print("✅ Azure SQL Database connection successful!")
            
            # Test table creation
            if connector.create_tables():
                print("✅ Database tables created/verified successfully!")
                print("\n📋 Next Steps:")
                print("1. Update config/azure_sql_config.json with your database details")
                print("2. Run with --azure-sql flag to use Azure SQL Database")
                print("3. Use --data-source azure-sql to load data from Azure SQL")
            else:
                print("❌ Failed to create/verify database tables")
        else:
            print("❌ Azure SQL Database connection failed!")
            print("\n🔧 Troubleshooting:")
            print("1. Check config/azure_sql_config.json settings")
            print("2. Verify Azure SQL Database firewall rules")
            print("3. Confirm ODBC Driver 17 for SQL Server is installed")
            
    except ImportError:
        print("❌ Azure SQL dependencies not installed")
        print("Install required packages: pip install pyodbc sqlalchemy azure-identity")
    except Exception as e:
        print(f"❌ Configuration test failed: {e}")

def load_csv_data():
    """Load data from CSV file, generate sample if not exists"""
    data_file = "data/sample_transactions.csv"
    
    if not os.path.exists(data_file):
        print("📝 Sample data not found, generating new sample data...")
        os.makedirs("data", exist_ok=True)
        
        # Generate sample data
        sample_df = generate_sample_data(num_records=1000)
        sample_df.to_csv(data_file, index=False)
        print(f"✓ Generated {len(sample_df)} sample transactions")
    
    return pd.read_csv(data_file)

def display_results(framework, results):
    """Display validation results"""
    print(f"\nStarting data quality validation for {results['total_input_records']} records...")
    
    validation_order = [
        'mandatory_fields', 'amount_range', 'currency_codes',
        'duplicate_transactions', 'timestamp_format', 'account_id_format'
    ]
    
    for check in validation_order:
        if check in framework.validation_results:
            print(f"✓ Checking {check.replace('_', ' ')}...")
    
    print(f"✓ Validation complete! {results['total_passed_records']} records passed all checks.")
    
    # Print summary
    print("\n" + "=" * 80)
    print("DATA QUALITY VALIDATION SUMMARY")
    print("=" * 80)
    print(f"📊 Total Input Records: {results['total_input_records']:,}")
    print(f"✅ Passed Records: {results['total_passed_records']:,}")
    print(f"❌ Failed Records: {results['total_failed_records']:,}")
    print(f"📈 Overall Pass Rate: {results['overall_pass_rate']:.2%}")
    print(f"🏆 Quality Status: {framework.summary_stats['quality_status']}")
    print(f"⏰ Validation Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Print detailed results
    print(f"\n📋 Detailed Results by Check:")
    print("-" * 80)
    summary_df = framework.get_validation_summary()
    print(summary_df.to_string(index=False))
    print("=" * 80)

def display_key_metrics(framework, report_files):
    """Display key metrics and file locations"""
    print(f"\n🎯 Key Metrics:")
    print(f"   • Data Quality Score: {framework.summary_stats.get('overall_pass_rate', 0)*100:.1f}%")
    print(f"   • Quality Status: {framework.summary_stats.get('quality_status', 'UNKNOWN')}")
    print(f"   • Clean Records: {framework.summary_stats.get('total_passed_records', 0):,}")
    print(f"   • Failed Records: {framework.summary_stats.get('total_failed_records', 0):,}")
    
    print(f"\n📁 Generated Files:")
    print(f"   • Sample Data: data/sample_transactions.csv")
    print(f"   • Failed Records: data/failed_records/")
    print(f"   • Reports: data/reports/")
    
    print(f"\n🔗 Next Steps:")
    print(f"   1. Review failed records in data/failed_records/")
    print(f"   2. Check detailed reports in data/reports/")
    print(f"   3. Run notebooks for deeper analysis")
    print(f"   4. Configure validation rules in config/")
    
    if framework.use_azure_sql:
        print(f"   5. Check Azure SQL Database for stored results")

if __name__ == "__main__":
    main()
