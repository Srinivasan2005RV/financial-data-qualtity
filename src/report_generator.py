"""
Report Generation Module for Data Quality Framework
Generates various reports including weekly summaries and detailed analysis
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import os
from typing import Dict, List, Any
from src.utils import create_directory_if_not_exists, export_to_excel


class DataQualityReportGenerator:
    """
    Generate comprehensive data quality reports
    """
    
    def __init__(self, output_dir: str = "data/reports"):
        """
        Initialize report generator
        
        Args:
            output_dir: Directory to save reports
        """
        self.output_dir = output_dir
        create_directory_if_not_exists(output_dir)
        
        # Set up plotting style
        plt.style.use('seaborn-v0_8')
        sns.set_palette("husl")
    
    def generate_weekly_report(self, validation_results: Dict[str, Any], 
                             week_start: datetime = None) -> str:
        """
        Generate comprehensive weekly data quality report
        
        Args:
            validation_results: Results from data quality framework
            week_start: Start date of the week (defaults to current week)
            
        Returns:
            Path to generated report file
        """
        if week_start is None:
            week_start = datetime.now() - timedelta(days=datetime.now().weekday())
        
        week_end = week_start + timedelta(days=6)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create report data structure
        report_data = {
            "Summary": self._create_summary_sheet(validation_results),
            "Validation_Details": self._create_validation_details_sheet(validation_results),
            "Failed_Records_Summary": self._create_failed_records_summary(validation_results),
            "Trends": self._create_trends_sheet(validation_results)
        }
        
        # Generate Excel report
        report_filename = f"Weekly_DQ_Report_{week_start.strftime('%Y%m%d')}_{timestamp}.xlsx"
        report_path = os.path.join(self.output_dir, report_filename)
        export_to_excel(report_data, report_path)
        
        # Generate visualizations
        self._generate_report_visualizations(validation_results, week_start)
        
        # Generate HTML summary
        html_report_path = self._generate_html_report(validation_results, week_start, week_end)
        
        print(f"✓ Weekly report generated: {report_path}")
        print(f"✓ HTML summary generated: {html_report_path}")
        
        return report_path
    
    def _create_summary_sheet(self, validation_results: Dict[str, Any]) -> pd.DataFrame:
        """Create executive summary sheet"""
        summary_stats = validation_results.get("summary_stats", {})
        
        summary_data = [
            ["Report Generation Date", datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
            ["Validation Timestamp", summary_stats.get("validation_timestamp", "N/A")],
            ["Total Input Records", f"{summary_stats.get('total_input_records', 0):,}"],
            ["Total Passed Records", f"{summary_stats.get('total_passed_records', 0):,}"],
            ["Total Failed Records", f"{summary_stats.get('total_failed_records', 0):,}"],
            ["Overall Pass Rate", f"{summary_stats.get('overall_pass_rate', 0):.2%}"],
            ["Quality Status", summary_stats.get('quality_status', 'UNKNOWN')],
            ["Checks Performed", ", ".join(summary_stats.get('checks_performed', []))],
        ]
        
        return pd.DataFrame(summary_data, columns=["Metric", "Value"])
    
    def _create_validation_details_sheet(self, validation_results: Dict[str, Any]) -> pd.DataFrame:
        """Create detailed validation results sheet"""
        validation_data = validation_results.get("validation_results", {})
        
        details = []
        for check_name, result in validation_data.items():
            details.append({
                "Validation_Check": check_name.replace("_", " ").title(),
                "Total_Records": result.get("total_records", 0),
                "Passed_Records": result.get("passed_count", 0),
                "Failed_Records": result.get("failed_count", 0),
                "Pass_Rate": f"{result.get('pass_rate', 0):.2%}",
                "Status": self._get_status_indicator(result.get('pass_rate', 0))
            })
        
        return pd.DataFrame(details)
    
    def _create_failed_records_summary(self, validation_results: Dict[str, Any]) -> pd.DataFrame:
        """Create summary of failed records by validation check"""
        failed_records = validation_results.get("failed_records", {})
        
        if not failed_records:
            return pd.DataFrame({"Message": ["No failed records found"]})
        
        summary_data = []
        for check_name, failed_df in failed_records.items():
            if len(failed_df) > 0:
                # Get most common failure reasons
                failure_reasons = failed_df['failure_reason'].value_counts()
                
                for reason, count in failure_reasons.items():
                    summary_data.append({
                        "Validation_Check": check_name.replace("_", " ").title(),
                        "Failure_Reason": reason,
                        "Failed_Count": count,
                        "Sample_Failed_Fields": failed_df[failed_df['failure_reason'] == reason]['failed_fields'].iloc[0] if 'failed_fields' in failed_df.columns else "N/A"
                    })
        
        return pd.DataFrame(summary_data)
    
    def _create_trends_sheet(self, validation_results: Dict[str, Any]) -> pd.DataFrame:
        """Create trends analysis sheet (placeholder for historical data)"""
        # This would typically involve historical data comparison
        # For now, we'll create a placeholder
        validation_data = validation_results.get("validation_results", {})
        
        trends_data = []
        for check_name, result in validation_data.items():
            trends_data.append({
                "Validation_Check": check_name.replace("_", " ").title(),
                "Current_Pass_Rate": f"{result.get('pass_rate', 0):.2%}",
                "Previous_Week": "N/A (Historical data not available)",
                "Trend": "→ Stable",
                "Recommendation": self._get_recommendation(result.get('pass_rate', 0))
            })
        
        return pd.DataFrame(trends_data)
    
    def _generate_report_visualizations(self, validation_results: Dict[str, Any], week_start: datetime):
        """Generate visualization charts for the report"""
        validation_data = validation_results.get("validation_results", {})
        
        if not validation_data:
            return
        
        # Create pass/fail rate chart
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 12))
        
        # 1. Pass rates by validation check
        checks = [check.replace("_", " ").title() for check in validation_data.keys()]
        pass_rates = [result.get('pass_rate', 0) * 100 for result in validation_data.values()]
        
        bars = ax1.bar(checks, pass_rates, color=['green' if rate >= 95 else 'orange' if rate >= 90 else 'red' for rate in pass_rates])
        ax1.set_title('Pass Rates by Validation Check')
        ax1.set_ylabel('Pass Rate (%)')
        ax1.tick_params(axis='x', rotation=45)
        ax1.set_ylim(0, 100)
        
        # Add value labels on bars
        for bar, rate in zip(bars, pass_rates):
            ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1, 
                    f'{rate:.1f}%', ha='center', va='bottom')
        
        # 2. Failed records count
        failed_counts = [result.get('failed_count', 0) for result in validation_data.values()]
        ax2.bar(checks, failed_counts, color='red', alpha=0.7)
        ax2.set_title('Failed Records by Validation Check')
        ax2.set_ylabel('Number of Failed Records')
        ax2.tick_params(axis='x', rotation=45)
        
        # 3. Overall quality status pie chart
        summary_stats = validation_results.get("summary_stats", {})
        passed = summary_stats.get('total_passed_records', 0)
        failed = summary_stats.get('total_failed_records', 0)
        
        if passed + failed > 0:
            ax3.pie([passed, failed], labels=['Passed', 'Failed'], autopct='%1.1f%%', 
                   colors=['green', 'red'], startangle=90)
            ax3.set_title('Overall Data Quality Distribution')
        
        # 4. Quality score gauge (simplified as bar)
        overall_pass_rate = summary_stats.get('overall_pass_rate', 0) * 100
        ax4.barh(['Quality Score'], [overall_pass_rate], color='green' if overall_pass_rate >= 95 else 'orange' if overall_pass_rate >= 90 else 'red')
        ax4.set_xlim(0, 100)
        ax4.set_xlabel('Quality Score (%)')
        ax4.set_title(f'Overall Quality Score: {overall_pass_rate:.1f}%')
        
        plt.tight_layout()
        
        # Save the plot
        chart_filename = f"DQ_Charts_{week_start.strftime('%Y%m%d')}.png"
        chart_path = os.path.join(self.output_dir, chart_filename)
        plt.savefig(chart_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"✓ Charts saved: {chart_path}")
    
    def _generate_html_report(self, validation_results: Dict[str, Any], 
                            week_start: datetime, week_end: datetime) -> str:
        """Generate HTML summary report"""
        summary_stats = validation_results.get("summary_stats", {})
        validation_data = validation_results.get("validation_results", {})
        
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Weekly Data Quality Report</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background-color: #f0f0f0; padding: 20px; border-radius: 5px; }}
                .summary {{ margin: 20px 0; }}
                .status-excellent {{ color: green; font-weight: bold; }}
                .status-warning {{ color: orange; font-weight: bold; }}
                .status-critical {{ color: red; font-weight: bold; }}
                table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
                .metric {{ font-size: 1.2em; margin: 10px 0; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>📊 Weekly Data Quality Report</h1>
                <p><strong>Report Period:</strong> {week_start.strftime('%Y-%m-%d')} to {week_end.strftime('%Y-%m-%d')}</p>
                <p><strong>Generated:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>
            
            <div class="summary">
                <h2>🎯 Executive Summary</h2>
                <div class="metric">📈 Total Records Processed: <strong>{summary_stats.get('total_input_records', 0):,}</strong></div>
                <div class="metric">✅ Records Passed: <strong>{summary_stats.get('total_passed_records', 0):,}</strong></div>
                <div class="metric">❌ Records Failed: <strong>{summary_stats.get('total_failed_records', 0):,}</strong></div>
                <div class="metric">📊 Overall Pass Rate: <strong>{summary_stats.get('overall_pass_rate', 0):.2%}</strong></div>
                <div class="metric">🏆 Quality Status: <span class="status-{summary_stats.get('quality_status', 'unknown').lower()}"><strong>{summary_stats.get('quality_status', 'UNKNOWN')}</strong></span></div>
            </div>
            
            <h2>📋 Detailed Validation Results</h2>
            <table>
                <tr>
                    <th>Validation Check</th>
                    <th>Total Records</th>
                    <th>Passed</th>
                    <th>Failed</th>
                    <th>Pass Rate</th>
                    <th>Status</th>
                </tr>
        """
        
        for check_name, result in validation_data.items():
            status_class = "excellent" if result.get('pass_rate', 0) >= 0.95 else "warning" if result.get('pass_rate', 0) >= 0.90 else "critical"
            html_content += f"""
                <tr>
                    <td>{check_name.replace('_', ' ').title()}</td>
                    <td>{result.get('total_records', 0):,}</td>
                    <td>{result.get('passed_count', 0):,}</td>
                    <td>{result.get('failed_count', 0):,}</td>
                    <td>{result.get('pass_rate', 0):.2%}</td>
                    <td class="status-{status_class}">{self._get_status_indicator(result.get('pass_rate', 0))}</td>
                </tr>
            """
        
        html_content += """
            </table>
            
            <h2>💡 Recommendations</h2>
            <ul>
        """
        
        # Add recommendations based on results
        for check_name, result in validation_data.items():
            if result.get('pass_rate', 0) < 0.90:
                html_content += f"<li><strong>{check_name.replace('_', ' ').title()}:</strong> {self._get_recommendation(result.get('pass_rate', 0))}</li>"
        
        html_content += """
            </ul>
            
            <div class="footer" style="margin-top: 40px; padding: 20px; background-color: #f9f9f9; border-radius: 5px;">
                <p><em>This report was automatically generated by the Data Quality Framework.</em></p>
                <p><em>For detailed failed records analysis, please refer to the Excel report and failed records CSV files.</em></p>
            </div>
        </body>
        </html>
        """
        
        # Save HTML report
        html_filename = f"Weekly_DQ_Report_{week_start.strftime('%Y%m%d')}.html"
        html_path = os.path.join(self.output_dir, html_filename)
        
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        return html_path
    
    def _get_status_indicator(self, pass_rate: float) -> str:
        """Get status indicator based on pass rate"""
        if pass_rate >= 0.95:
            return "✅ EXCELLENT"
        elif pass_rate >= 0.90:
            return "⚠️ WARNING"
        else:
            return "❌ CRITICAL"
    
    def _get_recommendation(self, pass_rate: float) -> str:
        """Get recommendation based on pass rate"""
        if pass_rate >= 0.95:
            return "Maintain current data quality standards."
        elif pass_rate >= 0.90:
            return "Monitor closely and investigate root causes of failures."
        else:
            return "Immediate investigation required. Review data sources and validation rules."
