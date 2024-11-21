#!/usr/bin/env python3
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import os
import time

# Connect to Elasticsearch
es = Elasticsearch(['http://127.0.0.1:9201'])

# Define the date range (last 30 days)
end_date = datetime.now()
start_date = end_date - timedelta(days=30)

# Query Elasticsearch for the document count per day
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
        "documents_per_day": {
            "date_histogram": {
                "field": "time",
                "calendar_interval": "day"
            }
        }
    },
    "size": 0
}

response = es.search(index="logger", body=query)

# Extract the date histogram buckets
buckets = response['aggregations']['documents_per_day']['buckets']

# Prepare data for plotting
dates = [datetime.utcfromtimestamp(bucket['key'] / 1000) for bucket in buckets]
counts = [bucket['doc_count'] for bucket in buckets]

# Function to format y-axis ticks as full numbers
def format_number(x, pos):
    return f'{int(x):,}'

# Plotting
plt.figure(figsize=(10, 6))
plt.plot(dates, counts, marker='o')
plt.xlabel('Date')
plt.ylabel('Request Count')
plt.title('Request Count per Day for the Last 30 Days')
plt.grid(True)
plt.xticks(dates, [date.strftime('%Y-%m-%d') for date in dates], rotation=45)
plt.gca().yaxis.set_major_formatter(FuncFormatter(format_number))
plt.ylim(0, max(counts) * 1.1)
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
