# Mini‑Project 2: Cloud Service Log Analytics (MapReduce → Ray)

## Overview
This project implements an end‑to‑end cloud log analytics pipeline using **cloud object storage**, **MapReduce baseline analytics**, and **Ray parallel processing**. The goal is to analyse synthetic cloud service logs to count requests and errors, identify slow endpoints, and detect degraded services under defined thresholds.

The pipeline follows the required workflow:
**Cloud service log dataset → Cloud object storage → MapReduce baseline analytics → Ray extension analytics → Comparison**

## Dataset
- Synthetic cloud service log dataset with **50,000 records**
- Fields: `timestamp, request_id, user_id, service_name, endpoint, http_method, status_code, response_time_ms, region, error_type`
- Metrics: service‑level requests/errors, slow requests (>800ms), timeout errors

## Project Structure
```
/
├── data/                # Anonymised log dataset
├── mapreduce/           # MapReduce mapper/reducer scripts
│   └── mapreduce_results.txt
├── ray/                 # Ray parallel detection scripts
│   └── ray_results.txt
├── docs/                # Anonymised screenshots & evidence
└── README.md
```

## Environment & Dependencies
- Cloud storage: **Alibaba Cloud OSS** (free tier)
- MapReduce: Local Hadoop environment
- Ray: Local Ray mode
- Language: Python 3

## How to Run
### 1. Cloud Object Storage
- Upload the dataset to an OSS bucket
- Use the anonymised path for downstream access

### 2. MapReduce Jobs
```bash
# Run MapReduce for request count, error count, top slow endpoints
hadoop jar job.jar input output
# Results saved to mapreduce/mapreduce_results.txt
```

### 3. Ray Degraded Service Detection
```bash
# Run parallel Ray task
python ray_degraded_detection.py
# Results saved to ray/ray_results.txt
```

## Key Results
### MapReduce Outputs
- Request count per service
- Server error count (status ≥500) per service
- Top 10 slow endpoints (>800ms)

### Ray Outputs (Degraded Services)
```
search-service,slow_rate_43.2%>20%; timeout_472>=5
order-service,timeout_341>=5
payment-service,slow_rate_27.0%>20%; error_rate_17.2%>10%; timeout_721>=5
```

## Validation
All results were validated by manual calculation and cross‑checking between MapReduce and Ray statistics. The pipeline ensures data consistency and correct threshold enforcement for degraded service detection.

## Authors
Group project, with equal contributions from Member A, Member B, and Member C.

---

Code part

---

## Project Structure

```
CloudComputing_miniP2/
│
├── MiniProject 2 Dataset.csv    # Input dataset (50,000 log entries)
│
├── MapReduce.py                            # MapReduce baseline analysis
├── mapreduce_results.txt                   # MapReduce output results
│
├── Ray.py                                  # Ray degraded service detection
├── ray_results.txt                         # Ray output results
│
└── README.md                               # This file
```

---

## Dataset Description

Synthetic cloud service log data with **50,000 entries** across **10 fields**:

| # | Field | Description |
|---|-------|-------------|
| 1 | timestamp | Request timestamp |
| 2 | request_id | Unique request identifier |
| 3 | user_id | User identifier |
| 4 | service_name | Service name (auth/order/payment/search/notification) |
| 5 | endpoint | API endpoint path |
| 6 | http_method | HTTP method (GET/POST/PUT/DELETE) |
| 7 | status_code | HTTP status code |
| 8 | response_time_ms | Response time in milliseconds |
| 9 | region | Service region |
| 10 | error_type | Error category (timeout/error/none) |

### Services Analyzed
- `auth-service`
- `order-service`
- `payment-service`
- `search-service`
- `notification-service`

---

## Environment Setup

### Prerequisites
```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# Install dependencies
pip install mrjob ray
System Requirements
Python 3.8+

mrjob (for MapReduce)

Ray (for parallel processing)

4+ CPU cores recommended

Task 2: MapReduce Baseline Analysis
File: MapReduce.py
Description
Implements MapReduce processing using mrjob library to perform three analytical tasks on cloud service logs.

MapReduce Job Structure
text
Step 1: Mapper → Reducer (Aggregation)
Step 2: Reducer only (Top 10 Selection)
Three Outputs
Output	Description	Key Format
Output 1	Request count per service	REQUEST_COUNT: <service>
Output 2	Server error count per service (status ≥ 500)	ERROR_COUNT: <service>
Output 3	Top 10 slowest endpoints (>800ms)	TOP_10_SLOW: <service>,<endpoint>
Design Decisions
Two-step processing: Step 1 aggregates per-service metrics; Step 2 extracts Top 10 using heapq.nlargest() for O(n log k) efficiency

Composite keys: (service, metric_type) for efficient grouping in reducers

Configurable threshold: --slow-threshold parameter (default: 800ms)

Error handling: Skips malformed lines gracefully

Usage
bash
# Run locally
python MapReduce.py "MiniProject 2 Dataset.csv"

# Save results to file
python MapReduce.py "MiniProject 2 Dataset.csv" > mapreduce_results.txt

# Custom slow threshold
python MapReduce.py --slow-threshold 1000 "MiniProject 2 Dataset.csv"
Results Summary
Service	Requests	Errors (≥500)
auth-service	12,121	436
notification-service	8,412	436
order-service	10,937	717
payment-service	7,914	1,362
search-service	10,616	904
Top 3 Slowest Endpoints:

search-service, /search/results (1,174 slow requests)

search-service, /search/filter (1,165 slow requests)

search-service, /search (1,123 slow requests)

Task 3: Ray Extended Analysis
File: Ray.py
Description
Implements parallel processing using Ray to detect degraded services based on three conditions.

Ray Architecture
text
Dataset → 4 Partitions → @ray.remote parallel tasks → Merge → Degradation Detection
Degradation Detection Rules
A service is degraded if ANY of the following conditions are met:

Condition	Threshold	Metric Calculation
Slow Request Rate	> 20%	slow_requests / total_requests
Server Error Rate	> 10%	errors_500 / total_requests
Timeout Count	≥ 5	count of timeout errors
Key Ray Features Used
@ray.remote decorator for remote task definition

Parallel execution across 4 partitions

ray.get() for result collection

Local result merging for global statistics

Usage
bash
# Run Ray analysis
python Ray.py

# Results automatically saved to ray_results.txt
Results Summary
All 5 services detected as degraded:

Service	Degradation Reasons
search-service	Slow rate 43.2% > 20%, Timeout 472 ≥ 5
order-service	Timeout 341 ≥ 5
payment-service	Slow rate 27.0% > 20%, Error rate 17.2% > 10%, Timeout 721 ≥ 5
notification-service	Timeout 186 ≥ 5
auth-service	Timeout 217 ≥ 5
Comparison: MapReduce vs Ray
Aspect	MapReduce (mrjob)	Ray
Programming Model	Map/Reduce functions	Task-based (Python functions)
Parallelism	Disk-based, batch processing	In-memory, fine-grained
Ease of Use	Requires map/reduce mental model	Familiar Python functions
Performance	Higher latency (disk I/O)	Lower latency (memory)
Data Flow	Shuffle between phases	Direct object references
Scalability	Horizontally to large clusters	Horizontally with auto-scaling
Use Case	Batch processing of large datasets	Real-time/interactive analytics
Setup Complexity	Hadoop cluster or local runner	pip install ray + ray.init()
Key Takeaways
MapReduce is suitable for large-scale batch processing with well-defined aggregation patterns

Ray excels at flexible parallel computation with lower latency and simpler programming model

For this 50K-row dataset, both frameworks complete in under 30 seconds

Ray's in-memory model is more efficient for iterative or multi-condition analysis

Cloud Object Storage (Task 1)
Storage Choice: Amazon S3 / Alibaba Cloud OSS
Why Object Storage for Log Data?
Scalability: Automatically scales to handle TB/PB of log data

Durability: 99.999999999% (11 9's) data durability

Cost-effective: Pay-per-use pricing, lifecycle policies for archival

Availability: Accessible from any compute service (EC2, EMR, ECS)

Integration: Native integration with Hadoop/Spark/Ray ecosystems

Versioning: Built-in object versioning for log integrity

Upload Command (AWS S3 Example)
bash
aws s3 cp "MiniProject 2 Dataset.csv" s3://compxxxxj-logs/input/
