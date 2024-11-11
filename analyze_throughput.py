# analyze_throughput.py
import pandas as pd
import numpy as np
from datetime import datetime

def analyze_log(logfile):
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
        'throughput': throughputs
    })
    
    # Calculate statistics
    stats = {
        'min_throughput': df['throughput'].min(),
        'max_throughput': df['throughput'].max(),
        'mean_throughput': df['throughput'].mean(),
        'std_dev': df['throughput'].std(),
        'last_minute_avg': df.tail(60)['throughput'].mean()
    }
    
    return stats

print("Log Analysis Results:")
print(analyze_log('benchmark_logs/latest/consumer_16_nodes.log'))