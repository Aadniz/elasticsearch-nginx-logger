#!/usr/bin/env python3
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import os
import time

# Connect to Elasticsearch
es = Elasticsearch(['http://localhost:9201'], request_timeout=600)  # It's a heavy request, needs 60 secs

# Define the date range (last 30 days)
end_date = datetime.now()
start_date = end_date - timedelta(days=30)

# Query Elasticsearch for the unique visitors per day
query = {
    "query": {
        "range": {
            "time": {
                "gte": start_date,
                "lte": end_date,
                "format": "strict_date_optional_time||epoch_millis"
            }
        }
    },
    "aggs": {
        "unique_visitors_per_day": {
            "date_histogram": {
                "field": "time",
                "calendar_interval": "day"
            },
            "aggs": {
                "unique_ips": {
                    "cardinality": {
                        "field": "ip"
                    }
                }
            }
        }
    },
    "size": 0
}

response = es.search(index="logger", body=query)

# Extract the date histogram buckets
buckets = response['aggregations']['unique_visitors_per_day']['buckets']

# Prepare data for plotting
dates = [datetime.utcfromtimestamp(bucket['key'] / 1000) for bucket in buckets]
unique_visitors = [bucket['unique_ips']['value'] for bucket in buckets]

# Function to format y-axis ticks as full numbers
def format_number(x, pos):
    return f'{int(x):,}'

# Plotting
plt.figure(figsize=(10, 6))
plt.plot(dates, unique_visitors, marker='o')
plt.xlabel('Date')
plt.ylabel('Unique IP Count')
plt.title('Unique IPs per Day for the Last 30 Days')
plt.grid(True)
plt.xticks(dates, [date.strftime('%Y-%m-%d') for date in dates], rotation=45)
plt.gca().yaxis.set_major_formatter(FuncFormatter(format_number))
plt.ylim(0, max(unique_visitors) * 1.1)
plt.tight_layout()

# Get the absolute path of the script directory
script_dir = os.path.dirname(os.path.abspath(__file__))

# Create plots directory if it doesn't exist
plots_dir = os.path.join(script_dir, "plots")
os.makedirs(plots_dir, exist_ok=True)

# Get the script name without .py and current timestamp
script_name = os.path.basename(__file__).replace('.py', '')
timestamp = time.strftime("%Y%m%d_%H%M%S")

# Save the plot as an SVG file
file_path = os.path.join(plots_dir, f"{script_name}_{timestamp}.svg")
plt.savefig(file_path)

print(f"Plot saved as {file_path}")

# Show the plot
plt.show()
