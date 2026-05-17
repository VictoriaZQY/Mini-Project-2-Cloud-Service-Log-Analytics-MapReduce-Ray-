# Mini‑Project 2: Cloud Service Log Analytics (MapReduce → Ray)

## Overview
This project implements an end‑to‑end cloud log analytics pipeline using **cloud object storage**, **MapReduce baseline analytics**, and **Ray parallel processing**. The goal is to analyse synthetic cloud service logs to count requests and errors, identify slow endpoints, and detect degraded services under defined thresholds.

The pipeline follows the required workflow:
**Cloud service log dataset → Cloud object storage → MapReduce baseline analytics → Ray extension analytics → Comparison**

## Dataset
- Synthetic cloud service log dataset with **50,000 records**
- Fields: `timestamp, request_id, user_id, service_name, endpoint, http_method, status_code, response_time_ms, region, error_type`
- Metrics: service‑level requests/errors, slow requests (>800ms), timeout errors
- Link: https://comp3041j-l0g-2026.oss-cn-beijing.aliyuncs.com/Comp3041J%20MiniProject%202%20Dataset.csv?Expires=1778204077&0SSAccessKed=TMP.3Kx9taM9QsZZf9TMaRep3vxvfxyS4VfWQwhmHnMjBBdvyUTszfWnmTWRjphBuoUkVaSkxgS8Dd15PeV8cL8yfRdKhsq8Wc&Signature=NY3ea7IXrzF104KQ3C60tBvMY4%3D

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
