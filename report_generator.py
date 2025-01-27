import os
from datetime import datetime
import json
import plotly.graph_objects as go
from plotly.subplots import make_subplots

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Kafka Benchmark Results - {timestamp}</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 40px;
            background-color: #f5f5f5;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background-color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 0 10px rgba(0,0,0,0.1);
        }
        h1 {
            color: #333;
            border-bottom: 2px solid #eee;
            padding-bottom: 10px;
        }
        .summary {
            margin: 20px 0;
            padding: 15px;
            background-color: #f8f9fa;
            border-radius: 4px;
        }
        .chart {
            margin: 20px 0;
            padding: 15px;
            background-color: white;
            border: 1px solid #ddd;
            border-radius: 4px;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }
        th, td {
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }
        th {
            background-color: #f4f4f4;
        }
        tr:hover {
            background-color: #f5f5f5;
        }
    </style>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
</head>
<body>
    <div class="container">
        <h1>Kafka Benchmark Results</h1>
        <div class="summary">
            <h2>Test Summary</h2>
            <p>Date: {timestamp}</p>
            <p>Total Scales Tested: {num_scales}</p>
        </div>
        {charts_div}
        <div class="detailed-results">
            <h2>Detailed Results</h2>
            {results_table}
        </div>
    </div>
</body>
</html>
"""

def generate_charts(results):
    """Generate Plotly charts for the results"""
    scales = list(map(int, results.keys()))
    
    # Create subplots
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=("Throughput (Producer)", "Throughput (Consumer)",
                       "Latency (Producer)", "Latency (Consumer)")
    )
    
    # Producer throughput
    fig.add_trace(
        go.Scatter(x=scales, 
                  y=[results[str(s)]['producer']['throughput_msgs_per_sec'] for s in scales],
                  name="Producer Throughput",
                  mode='lines+markers'),
        row=1, col=1
    )
    
    # Consumer throughput
    fig.add_trace(
        go.Scatter(x=scales, 
                  y=[results[str(s)]['consumer']['throughput_msgs_per_sec'] for s in scales],
                  name="Consumer Throughput",
                  mode='lines+markers'),
        row=1, col=2
    )
    
    # Producer latency
    fig.add_trace(
        go.Scatter(x=scales, 
                  y=[results[str(s)]['producer']['avg_latency_ms'] for s in scales],
                  name="Producer Avg Latency",
                  mode='lines+markers'),
        row=2, col=1
    )
    
    # Consumer latency
    fig.add_trace(
        go.Scatter(x=scales, 
                  y=[results[str(s)]['consumer']['avg_latency_ms'] for s in scales],
                  name="Consumer Avg Latency",
                  mode='lines+markers'),
        row=2, col=2
    )
    
    fig.update_layout(height=800, showlegend=True)
    return fig.to_html(include_plotlyjs=False)

def generate_results_table(results):
    """Generate HTML table from results"""
    table_html = """
    <table>
        <tr>
            <th>Scale</th>
            <th>Messages Sent</th>
            <th>Producer Throughput</th>
            <th>Producer Avg Latency</th>
            <th>Messages Received</th>
            <th>Consumer Throughput</th>
            <th>Consumer Avg Latency</th>
        </tr>
    """
    
    for scale in sorted(map(int, results.keys())):
        scale_str = str(scale)
        r = results[scale_str]
        table_html += f"""
        <tr>
            <td>{scale}</td>
            <td>{r['producer']['messages_sent']:,}</td>
            <td>{r['producer']['throughput_msgs_per_sec']:,.2f}</td>
            <td>{r['producer']['avg_latency_ms']:,.2f} ms</td>
            <td>{r['consumer']['messages_received']:,}</td>
            <td>{r['consumer']['throughput_msgs_per_sec']:,.2f}</td>
            <td>{r['consumer']['avg_latency_ms']:,.2f} ms</td>
        </tr>
        """
    
    table_html += "</table>"
    return table_html

def generate_report(results, output_dir):
    """Generate HTML report from benchmark results"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate charts and table
    charts_html = generate_charts(results)
    results_table = generate_results_table(results)
    
    # Fill template
    report_html = HTML_TEMPLATE.format(
        timestamp=timestamp,
        num_scales=len(results),
        charts_div=charts_html,
        results_table=results_table
    )
    
    # Save report
    report_filename = f"benchmark_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
    report_path = os.path.join(output_dir, report_filename)
    
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report_html)
    
    return report_path
