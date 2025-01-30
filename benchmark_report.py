import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
from datetime import datetime

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Kafka Benchmark Results</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        .container { max-width: 1200px; margin: 0 auto; }
        .header { text-align: center; margin-bottom: 30px; }
        .summary { margin: 20px 0; }
        .chart { margin: 30px 0; }
        table { border-collapse: collapse; width: 100%; margin: 20px 0; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Kafka Benchmark Results</h1>
            <p>{timestamp}</p>
        </div>
        <div class="summary">
            <h2>Test Configuration</h2>
            {config_table}
        </div>
        <div class="chart">
            <h2>Performance Metrics</h2>
            {throughput_latency_plot}
        </div>
        <div class="chart">
            <h2>Latency Distribution</h2>
            {latency_plot}
        </div>
        <div class="summary">
            <h2>Detailed Results</h2>
            {results_table}
        </div>
    </div>
</body>
</html>
"""

def create_performance_plot(results):
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    x = [r['num_servers'] for r in results]
    
    # Throughput line
    fig.add_trace(
        go.Scatter(x=x, y=[r['messages_per_second'] for r in results],
                  name="Messages/sec", mode='lines+markers'),
        secondary_y=False
    )
    
    # Average latency line
    fig.add_trace(
        go.Scatter(x=x, y=[r['avg_latency_ms'] for r in results],
                  name="Avg Latency (ms)", mode='lines+markers'),
        secondary_y=True
    )
    
    fig.update_layout(
        title="Throughput vs Latency",
        xaxis_title="Number of Servers",
        xaxis_type="log"
    )
    
    fig.update_yaxes(title_text="Messages per Second", secondary_y=False)
    fig.update_yaxes(title_text="Latency (ms)", secondary_y=True)
    
    return fig.to_html(full_html=False)

def create_latency_plot(results):
    fig = go.Figure()
    
    x = [r['num_servers'] for r in results]
    
    fig.add_trace(go.Scatter(x=x, y=[r['avg_latency_ms'] for r in results],
                            name="Avg Latency", mode='lines+markers'))
    fig.add_trace(go.Scatter(x=x, y=[r['p95_latency_ms'] for r in results],
                            name="P95 Latency", mode='lines+markers'))
    fig.add_trace(go.Scatter(x=x, y=[r['p99_latency_ms'] for r in results],
                            name="P99 Latency", mode='lines+markers'))
    
    fig.update_layout(
        title="Latency Distribution",
        xaxis_title="Number of Servers",
        yaxis_title="Latency (ms)",
        xaxis_type="log"
    )
    
    return fig.to_html(full_html=False)

def create_config_table(config):
    rows = []
    for key, value in config.items():
        rows.append(f"<tr><td>{key}</td><td>{value}</td></tr>")
    return f"<table><tr><th>Parameter</th><th>Value</th></tr>{''.join(rows)}</table>"

def create_results_table(results):
    headers = ["Servers", "Messages/sec", "Avg Latency (ms)", "P95 Latency (ms)", "P99 Latency (ms)"]
    rows = []
    for r in results:
        row = [
            f"<td>{r['num_servers']}</td>",
            f"<td>{r['messages_per_second']:.2f}</td>",
            f"<td>{r['avg_latency_ms']:.2f}</td>",
            f"<td>{r['p95_latency_ms']:.2f}</td>",
            f"<td>{r['p99_latency_ms']:.2f}</td>"
        ]
        rows.append(f"<tr>{''.join(row)}</tr>")
    
    return f"<table><tr><th>{'</th><th>'.join(headers)}</th></tr>{''.join(rows)}</table>"

def generate_html_report(results, config_data, timestamp):
    config_table = create_config_table(config_data)
    throughput_latency_plot = create_performance_plot(results)
    latency_plot = create_latency_plot(results)
    results_table = create_results_table(results)
    
    html_content = HTML_TEMPLATE.format(
        timestamp=timestamp,
        config_table=config_table,
        throughput_latency_plot=throughput_latency_plot,
        latency_plot=latency_plot,
        results_table=results_table
    )
    
    return html_content
