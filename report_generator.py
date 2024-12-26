# report_generator.py
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
from datetime import datetime
import numpy as np
from pathlib import Path
from scipy import stats

class BenchmarkReportGenerator:
    def __init__(self, results_dir):
        self.results_dir = Path(results_dir)
        self.output_dir = self.results_dir / 'report'
        self.output_dir.mkdir(exist_ok=True)
        try:
            # Try to use seaborn style if available
            import seaborn as sns
            plt.style.use('seaborn')
        except (ImportError, OSError):
            # Fallback to a built-in style if seaborn is not available
            plt.style.use('bmh')  # A good alternative built-in style
        # Define a color palette
        self.colors = sns.color_palette("husl", 8)
        self.line_styles = ['-', '--', '-.', ':']

    def load_data(self):
        """Load all CSV files from the benchmark results directory"""
        data = {}
        for csv_file in self.results_dir.glob('**/detailed_metrics_*.csv'):
            # Extract server count from filename
            num_servers = int(csv_file.stem.split('_')[-1])
            df = pd.read_csv(csv_file)
            data[num_servers] = df
        # Sort data by number of servers
        return dict(sorted(data.items()))

    def generate_throughput_plot(self, data):
        """Generate throughput over time plot for different server counts"""
        plt.figure(figsize=(12, 6))
        for idx, (num_servers, df) in enumerate(data.items()):
            color = self.colors[idx % len(self.colors)]
            style = self.line_styles[idx % len(self.line_styles)]
            plt.plot(df['timestamp'] - df['timestamp'].min(), 
                    df['rate'].rolling(window=5).mean(),  # Add smoothing
                    label=f'{num_servers} servers',
                    color=color,
                    linestyle=style,
                    linewidth=2)
        
        plt.xlabel('Time (seconds)', fontsize=12)
        plt.ylabel('Messages/Second', fontsize=12)
        plt.title('Throughput Over Time by Server Count', fontsize=14, pad=20)
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(self.output_dir / 'throughput.png', dpi=300, bbox_inches='tight')
        plt.close()

    def generate_resource_plots(self, data):
        """Generate CPU and memory usage plots"""
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))

        for num_servers, df in data.items():
            ax1.plot(df['timestamp'] - df['timestamp'].min(), 
                    df['cpu_usage'],
                    label=f'{num_servers} servers')
            
            ax2.plot(df['timestamp'] - df['timestamp'].min(),
                    df['mem_usage'],
                    label=f'{num_servers} servers')

        ax1.set_xlabel('Time (seconds)')
        ax1.set_ylabel('CPU Usage (%)')
        ax1.set_title('CPU Usage Over Time')
        ax1.legend(bbox_to_anchor=(1.05, 1), loc='upper left')

        ax2.set_xlabel('Time (seconds)')
        ax2.set_ylabel('Memory Usage (%)')
        ax2.set_title('Memory Usage Over Time')
        ax2.legend(bbox_to_anchor=(1.05, 1), loc='upper left')

        plt.tight_layout()
        plt.savefig(self.output_dir / 'resource_usage.png')
        plt.close()

    def generate_statistics(self, data):
        """Generate statistical summary of the benchmark results"""
        stats = []
        for num_servers, df in data.items():
            # Calculate additional statistics
            rate_stability = 1 - (df['rate'].std() / df['rate'].mean())
            cpu_efficiency = df['rate'].mean() / df['cpu_usage'].mean()
            
            stats.append({
                'num_servers': num_servers,
                'avg_rate': df['rate'].mean(),
                'max_rate': df['rate'].max(),
                'min_rate': df['rate'].min(),
                'std_rate': df['rate'].std(),
                'avg_cpu': df['cpu_usage'].mean(),
                'avg_mem': df['mem_usage'].mean(),
                'p95_rate': df['rate'].quantile(0.95),
                'p99_rate': df['rate'].quantile(0.99),
                'stability_score': rate_stability * 100,
                'cpu_efficiency': cpu_efficiency,
                'duration_seconds': df['timestamp'].max() - df['timestamp'].min()
            })
        return pd.DataFrame(stats)

    def generate_comparison_table(self, stats_df):
        """Generate a comparison table between different server configurations"""
        baseline = stats_df.iloc[0]['avg_rate']
        stats_df['scaling_factor'] = stats_df['avg_rate'] / baseline
        stats_df['efficiency_ratio'] = stats_df['scaling_factor'] / stats_df['num_servers']
        return stats_df

    def generate_html_report(self, stats_df):
        """Generate HTML report with plots and statistics"""
        comparison_df = self.generate_comparison_table(stats_df)
        
        html_template = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Benchmark Results Report</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 40px; background-color: #f5f5f5; }}
                .container {{ max-width: 1200px; margin: 0 auto; background-color: white; padding: 20px; border-radius: 10px; box-shadow: 0 0 10px rgba(0,0,0,0.1); }}
                .plot {{ margin: 20px 0; text-align: center; background-color: white; padding: 15px; border-radius: 5px; }}
                .plot img {{ max-width: 100%; border-radius: 5px; }}
                table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
                th, td {{ border: 1px solid #ddd; padding: 12px; text-align: right; }}
                th {{ background-color: #f8f9fa; }}
                tr:nth-child(even) {{ background-color: #f8f9fa; }}
                tr:hover {{ background-color: #f2f2f2; }}
                h1, h2 {{ color: #333; border-bottom: 2px solid #ddd; padding-bottom: 10px; }}
                .summary-box {{ background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin: 20px 0; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Benchmark Results Report</h1>
                <p>Generated on: {timestamp}</p>
                
                <div class="summary-box">
                    <h2>Key Findings</h2>
                    <ul>
                        <li>Best performing configuration: {best_config}</li>
                        <li>Maximum throughput achieved: {max_throughput:.2f} msg/sec</li>
                        <li>Best efficiency ratio: {best_efficiency:.2f}</li>
                    </ul>
                </div>

                <h2>Throughput Analysis</h2>
                <div class="plot">
                    <img src="throughput.png" alt="Throughput Plot">
                </div>
                
                <h2>Resource Usage</h2>
                <div class="plot">
                    <img src="resource_usage.png" alt="Resource Usage Plot">
                </div>
                
                <h2>Performance Statistics</h2>
                {stats_table}

                <h2>Scaling Analysis</h2>
                {comparison_table}
            </div>
        </body>
        </html>
        """
        
        # Convert stats DataFrame to HTML table
        stats_table = stats_df.round(2).to_html(classes='stats-table')
        comparison_table = comparison_df.round(2).to_html(classes='comparison-table')
        best_config = f"{stats_df.loc[stats_df['avg_rate'].idxmax(), 'num_servers']} servers"
        max_throughput = stats_df['avg_rate'].max()
        best_efficiency = comparison_df['efficiency_ratio'].max()
        
        # Generate report
        report_html = html_template.format(
            timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            stats_table=stats_table,
            comparison_table=comparison_table,
            best_config=best_config,
            max_throughput=max_throughput,
            best_efficiency=best_efficiency
        )
        
        with open(self.output_dir / 'report.html', 'w') as f:
            f.write(report_html)

    def generate_report(self):
        """Main method to generate the complete report"""
        data = self.load_data()
        if not data:
            raise ValueError("No benchmark data found in the specified directory")

        # Generate plots
        self.generate_throughput_plot(data)
        self.generate_resource_plots(data)
        
        # Generate statistics
        stats_df = self.generate_statistics(data)
        
        # Generate HTML report
        self.generate_html_report(stats_df)
        
        print(f"Report generated successfully in {self.output_dir}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate benchmark report')
    parser.add_argument('results_dir', help='Directory containing benchmark results')
    args = parser.parse_args()
    
    generator = BenchmarkReportGenerator(args.results_dir)
    generator.generate_report()