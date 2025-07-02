"""
Main execution script for Data Quality Framework
Run this script to perform data quality validation on financial transaction data
"""

import sys
import os
import pandas as pd
from datetime import datetime

# Add src directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.data_quality_framework import DataQualityFramework
from src.report_generator import DataQualityReportGenerator
from src.utils import generate_sample_data, create_directory_if_not_exists


def main():
    """
    Main function to run data quality validation
    """
    print("🚀 Starting Data Quality Framework for Financial Data")
    print("=" * 60)
    
    # Initialize framework
    dq_framework = DataQualityFramework()
    report_generator = DataQualityReportGenerator()
    
    # Check if sample data exists, if not generate it
    sample_data_path = "data/sample_transactions.csv"
    
    if not os.path.exists(sample_data_path):
        print("📊 Generating sample transaction data...")
        create_directory_if_not_exists("data")
        
        # Generate sample data with some errors for testing
        sample_df = generate_sample_data(num_records=1000, include_errors=True)
        sample_df.to_csv(sample_data_path, index=False)
        print(f"✓ Sample data saved to: {sample_data_path}")
    else:
        print(f"📁 Loading existing data from: {sample_data_path}")
    
    # Load the transaction data
    try:
        df = pd.read_csv(sample_data_path)
        print(f"✓ Loaded {len(df)} transactions for validation")
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return
    
    # Run all validations
    print("\n🔍 Running Data Quality Validations...")
    print("-" * 40)
    
    validation_results = dq_framework.run_all_validations(df)
    
    # Print summary
    dq_framework.print_summary()
    
    # Save failed records
    if dq_framework.failed_records:
        print("\n💾 Saving failed records for inspection...")
        dq_framework.save_failed_records()
    
    # Generate reports
    print("\n📋 Generating Data Quality Reports...")
    print("-" * 40)
    
    try:
        report_path = report_generator.generate_weekly_report(validation_results)
        print(f"✓ Reports generated successfully!")
        
        # Display key metrics
        summary = dq_framework.summary_stats
        print(f"\n🎯 Key Metrics:")
        print(f"   • Data Quality Score: {summary['overall_pass_rate']:.1%}")
        print(f"   • Quality Status: {summary['quality_status']}")
        print(f"   • Clean Records: {summary['total_passed_records']:,}")
        print(f"   • Failed Records: {summary['total_failed_records']:,}")
        
    except Exception as e:
        print(f"⚠️ Warning: Report generation failed: {e}")
        print("Validation results are still available in memory.")
    
    # Show next steps
    print("\n📁 Generated Files:")
    print(f"   • Sample Data: {sample_data_path}")
    print(f"   • Failed Records: data/failed_records/")
    print(f"   • Reports: data/reports/")
    
    print("\n🔗 Next Steps:")
    print("   1. Review failed records in data/failed_records/")
    print("   2. Check detailed reports in data/reports/")
    print("   3. Run notebooks for deeper analysis")
    print("   4. Configure validation rules in config/")
    
    print("\n✅ Data Quality Framework execution completed!")
    
    return validation_results


if __name__ == "__main__":
    try:
        results = main()
    except KeyboardInterrupt:
        print("\n⏹️ Process interrupted by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
