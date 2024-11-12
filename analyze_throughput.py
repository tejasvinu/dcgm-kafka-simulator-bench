# analyze_throughput.py
import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
import glob
import os

def get_most_recent_logs_dir():
    base_dir = 'benchmark_logs'
    if not os.path.exists(base_dir):
        raise FileNotFoundError(f"Directory {base_dir} not found")
    
    dirs = [d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))]
    if not dirs:
        raise FileNotFoundError(f"No subdirectories found in {base_dir}")
    
    # Sort directories by creation time
    dirs.sort(key=lambda x: os.path.getctime(os.path.join(base_dir, x)), reverse=True)
    return os.path.join(base_dir, dirs[0])

def analyze_log(logfile, node_size):
    throughputs = []
    timestamps = []
    
    with open(logfile) as f:
        for line in f:
            if "Throughput:" in line:
                # Extract timestamp and throughput
                timestamp = datetime.strptime(line[:19], '%Y-%m-%d %H:%M:%S')
                throughput = float(line.split("Throughput: ")[1].split()[0])
                throughputs.append(throughput)
                timestamps.append(timestamp)
    
    df = pd.DataFrame({
        'timestamp': timestamps,
        'throughput': throughputs,
        'node_size': node_size
    })
    
    return df

def analyze_steady_state(df):
    # Remove warmup period (first 5 minutes)
    df = df[df['timestamp'] > df['timestamp'].min() + pd.Timedelta(minutes=5)]
    
    # Calculate steady-state metrics
    stats_by_node = df.groupby('node_size').agg({
        'throughput': ['mean', 'std', 'min', 'max', 
                      lambda x: stats.variation(x),  # CV
                      lambda x: stats.sem(x)]        # Standard error
    }).round(2)
    
    # Calculate confidence intervals
    confidence_intervals = df.groupby('node_size').agg({
        'throughput': lambda x: stats.t.interval(
            alpha=0.95,
            df=len(x)-1,
            loc=np.mean(x),
            scale=stats.sem(x)
        )
    })
    
    # Additional Arithmetic Analysis
    correlation = df.groupby('node_size').apply(
        lambda x: x['throughput'].corr(x['timestamp'].astype(int))
    ).rename('correlation')
    
    regression_results = df.groupby('node_size').apply(
        lambda x: stats.linregress(x['timestamp'].astype(int), x['throughput'])
    ).apply(pd.Series)
    
    return stats_by_node, confidence_intervals, correlation, regression_results

def plot_throughput_analysis(df):
    # Create subplot figure
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 15))
    
    # Time series plot
    sns.lineplot(data=df, x='timestamp', y='throughput', 
                hue='node_size', ax=ax1)
    ax1.set_title('Throughput Over Time')
    
    # Add regression lines
    for node in df['node_size'].unique():
        node_data = df[df['node_size'] == node]
        sns.regplot(x=node_data['timestamp'].astype(int), y=node_data['throughput'],
                    scatter=False, label=f'Regression {node} nodes', ax=ax1)
    ax1.legend()
    
    # Box plot
    sns.boxplot(data=df, x='node_size', y='throughput', ax=ax2)
    ax2.set_title('Throughput Distribution by Node Size')
    
    # Violin plot with points
    sns.violinplot(data=df, x='node_size', y='throughput', ax=ax3)
    ax3.set_title('Throughput Density by Node Size')
    
    plt.tight_layout()
    return fig

if __name__ == "__main__":
    try:
        logs_dir = get_most_recent_logs_dir()
        all_data = pd.DataFrame()
        log_files = glob.glob(os.path.join(logs_dir, 'consumer_*_nodes.log'))
        
        if not log_files:
            raise FileNotFoundError(f"No consumer log files found in {logs_dir}")
        
        for logfile in log_files:
            node_size = int(logfile.split('_')[-2])
            df = analyze_log(logfile, node_size)
            all_data = pd.concat([all_data, df], ignore_index=True)

        # Analyze steady state
        stats, ci, corr, reg = analyze_steady_state(all_data)
        print("Steady State Analysis:")
        print(stats)
        print("\nConfidence Intervals:")
        print(ci)
        print("\nCorrelation Coefficients:")
        print(corr)
        print("\nRegression Results:")
        print(reg)
        
        # Generate plots
        fig = plot_throughput_analysis(all_data)
        plt.savefig(os.path.join(logs_dir, 'throughput_analysis.png'))
        plt.close()
        
        # Save summary report
        with open(os.path.join(logs_dir, 'analysis_summary.txt'), 'w') as f:
            f.write("Steady State Analysis:\n")
            f.write(stats.to_string())
            f.write("\n\nConfidence Intervals:\n")
            f.write(ci.to_string())
            f.write("\n\nCorrelation Coefficients:\n")
            f.write(corr.to_string())
            f.write("\n\nRegression Results:\n")
            f.write(reg.to_string())
        
    except Exception as e:
        print(f"Error during analysis: {e}")
        raise