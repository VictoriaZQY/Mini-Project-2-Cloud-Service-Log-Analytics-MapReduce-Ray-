import ray
import csv
from collections import defaultdict

# Initialize Ray
ray.init(ignore_reinit_error=True)


@ray.remote
def process_partition(lines):
    """
    Remote task: process a batch of log lines, compute per-service metrics
    Input: lines - list of log lines
    Output: dict with service name as key and stats dict as value
    """
    service_stats = defaultdict(lambda: {
        'total': 0,
        'errors': 0,
        'slow': 0,
        'timeouts': 0
    })

    for line in lines:
        fields = line.strip().split(',')
        if fields[0] == 'timestamp':
            continue

        service = fields[3]
        status_code = int(fields[6])
        resp_time = float(fields[7])
        error_type = fields[9].strip() if len(fields) > 9 else ''

        # Total requests
        service_stats[service]['total'] += 1

        # Server-side errors (status >= 500)
        if status_code >= 500:
            service_stats[service]['errors'] += 1

        # Slow requests (response time > 800ms)
        if resp_time > 800:
            service_stats[service]['slow'] += 1

        # Timeout errors
        if 'timeout' in error_type.lower():
            service_stats[service]['timeouts'] += 1

    return {k: dict(v) for k, v in service_stats.items()}


def merge_stats(partition_results):
    """
    Merge statistics from multiple partitions
    """
    merged = defaultdict(lambda: {
        'total': 0,
        'errors': 0,
        'slow': 0,
        'timeouts': 0
    })

    for partition_result in partition_results:
        for service, stats in partition_result.items():
            merged[service]['total'] += stats['total']
            merged[service]['errors'] += stats['errors']
            merged[service]['slow'] += stats['slow']
            merged[service]['timeouts'] += stats['timeouts']

    return {k: dict(v) for k, v in merged.items()}


def detect_degraded_services(merged_stats):
    """
    Detect degraded services
    A service is degraded if ANY of the following holds:
    1. Slow request rate > 20%
    2. Server error rate > 10%
    3. Timeout count >= 5
    """
    results = []

    for service, stats in merged_stats.items():
        total = stats['total']
        if total == 0:
            continue

        reasons = []

        # Condition 1: slow rate > 20%
        slow_rate = stats['slow'] / total
        if slow_rate > 0.2:
            reasons.append(f"slow_rate_{slow_rate:.1%}>20%")

        # Condition 2: error rate > 10%
        error_rate = stats['errors'] / total
        if error_rate > 0.1:
            reasons.append(f"error_rate_{error_rate:.1%}>10%")

        # Condition 3: timeout count >= 5
        if stats['timeouts'] >= 5:
            reasons.append(f"timeout_{stats['timeouts']}>=5")

        if reasons:
            results.append((service, '; '.join(reasons)))

    return results


def main():
    input_file = "Comp3041J MiniProject 2 Dataset.csv"
    num_partitions = 4

    print("=" * 60)
    print("Ray Degraded Service Detection")
    print("=" * 60)

    # Step 1: read and partition log data
    print("\n[1] Reading and partitioning log data...")
    with open(input_file, 'r') as f:
        all_lines = f.readlines()

    header = all_lines[0]
    data_lines = all_lines[1:]

    partition_size = len(data_lines) // num_partitions
    partitions = []
    for i in range(num_partitions):
        start = i * partition_size
        end = start + partition_size if i < num_partitions - 1 else len(data_lines)
        partitions.append(data_lines[start:end])

    print(f"  Total rows: {len(data_lines)}")
    print(f"  Partitions: {num_partitions}")

    # Step 2: execute Ray remote tasks in parallel
    print("\n[2] Executing Ray remote tasks in parallel...")
    futures = [process_partition.remote(partition) for partition in partitions]

    partition_results = ray.get(futures)
    print(f"  Completed {len(partition_results)} partitions")

    # Step 3: merge global statistics
    print("\n[3] Merging partition statistics...")
    merged_stats = merge_stats(partition_results)

    print(f"\n{'Service':<25} {'Total':>8} {'Errors':>8} {'Slow':>8} {'Timeouts':>8}")
    print("-" * 65)
    for service, stats in merged_stats.items():
        print(f"{service:<25} {stats['total']:>8} {stats['errors']:>8} "
              f"{stats['slow']:>8} {stats['timeouts']:>8}")

    # Step 4: detect degraded services
    print("\n[4] Detecting degraded services...")
    degraded = detect_degraded_services(merged_stats)

    if degraded:
        print(f"\nFound {len(degraded)} degraded service(s):\n")
        print(f"{'Service':<25} {'Reason'}")
        print("-" * 70)
        for service, reason in degraded:
            print(f"{service:<25} {reason}")
    else:
        print("\nNo degraded services detected.")

    # Step 5: standard output format: service_name,reason
    print("\n[5] Standard output format (service_name,reason):\n")
    for service, reason in degraded:
        print(f"{service},{reason}")

    print("\n" + "=" * 60)
    print("Detection complete.")
    print("=" * 60)

    ray.shutdown()


if __name__ == "__main__":
    main()