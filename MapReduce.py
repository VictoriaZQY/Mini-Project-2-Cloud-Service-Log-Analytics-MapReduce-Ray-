"""
MapReduce Baseline Analysis for Cloud Service Log Analytics
Comp3041J Mini-Project 2 - Task 2

This MapReduce job performs three analyses on cloud service logs:
  1. Request count per service
  2. Server error count per service (status code >= 500)
  3. Top 10 slowest endpoints (response time > 800ms by default)

Input:  CSV log file with fields:
        timestamp, request_id, user_id, service_name, endpoint,
        http_method, status_code, response_time_ms, region, error_type

Usage:  python MapReduce.py "Comp3041J MiniProject 2 Dataset.csv"
"""

from mrjob.job import MRJob
from mrjob.step import MRStep
import heapq


class LogAnalysis(MRJob):
    """
    MapReduce job for cloud service log analysis.
    Uses two-step processing:
      Step 1: Mapper emits per-service counts; Reducer aggregates them.
      Step 2: Reducer extracts Top 10 slow endpoints using a max-heap.
    """

    def configure_args(self):
        """Add custom command-line arguments."""
        super(LogAnalysis, self).configure_args()
        self.add_passthru_arg(
            '--slow-threshold', type=int, default=800,
            help='Slow request threshold in milliseconds (default: 800)'
        )

    def steps(self):
        """
        Define the two MapReduce steps:
          Step 1: mapper -> reducer (aggregates per-service stats)
          Step 2: reducer only (extracts Top 10 slow endpoints)
        """
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.reducer_top10)
        ]

    def mapper(self, _, line):
        """
        Step 1 Mapper: Parse each log line and emit intermediate key-value pairs.

        Emits:
          - (service_name, "COUNT"): 1                    for request count
          - (service_name, "ERROR"): 1                    for server errors
          - ("SLOW_TOP_10", (service_name, endpoint)): resp_time  for slow requests
        """
        try:
            # Parse comma-separated log line
            fields = line.strip().split(',')

            # Skip header row and empty lines
            if not fields or fields[0] == 'timestamp':
                return

            # Extract relevant fields
            service = fields[3]       # service_name
            endpoint = fields[4]      # endpoint
            status_code = int(fields[6])   # HTTP status code
            resp_time = float(fields[7])   # response time in ms

            # Output 1: Count every request per service
            yield (service, "COUNT"), 1

            # Output 2: Count server errors (status code >= 500)
            if status_code >= 500:
                yield (service, "ERROR"), 1

            # Output 3: Flag slow requests for Top 10 ranking
            if resp_time > self.options.slow_threshold:
                yield ("SLOW_TOP_10", (service, endpoint)), resp_time

        except (IndexError, ValueError):
            # Skip malformed lines silently
            pass

    def reducer(self, key, values):
        """
        Step 1 Reducer: Aggregate values from mapper.

        For SLOW_TOP_10: counts occurrences per endpoint, sends to step 2.
        For COUNT/ERROR:  sums all values.
        """
        # Convert iterator to list for reuse
        values_list = list(values)

        if key[0] == "SLOW_TOP_10":
            # Count how many times this endpoint was slow
            # key[1] = (service_name, endpoint)
            yield "FINAL_SLOW_LIST", (len(values_list), key[1])
        else:
            # key = (service_name, metric)  ->  sum all counts
            yield key, sum(values_list)

    def reducer_top10(self, key, values):
        """
        Step 2 Reducer: Extract Top 10 slow endpoints and format all outputs.

        Uses heapq.nlargest for efficient Top-N selection.
        Formats final output with labeled keys.
        """
        # Convert iterator to list for reuse
        values_list = list(values)

        if key == "FINAL_SLOW_LIST":
            # Select Top 10 endpoints by slow request count
            # Each value: (count, (service_name, endpoint))
            top10 = heapq.nlargest(10, values_list, key=lambda x: x[0])
            for count, (service, endpoint) in top10:
                yield f"TOP_10_SLOW: {service},{endpoint}", count
        else:
            # Format COUNT and ERROR results with labels
            service, metric = key
            for value in values_list:
                if metric == "COUNT":
                    yield f"REQUEST_COUNT: {service}", value
                elif metric == "ERROR":
                    yield f"ERROR_COUNT: {service}", value


if __name__ == '__main__':
    LogAnalysis.run()