#!/usr/bin/env python3
"""T22 - Latency benchmark: direct vs proxy"""
import subprocess
import time
import statistics
import sys

sasl = ["-X", "security.protocol=SASL_PLAINTEXT", "-X", "sasl.mechanisms=PLAIN",
        "-X", "sasl.username=admin", "-X", "sasl.password=admin-secret",
        "-X", "message.timeout.ms=5000"]

msgs = [f"bench-{i}-{int(time.time()*1e9)}" for i in range(50)]

def run_bench(broker, label, iterations=50):
    times = []
    for i in range(iterations):
        t0 = time.time()
        p = subprocess.run(
            ["kcat", "-b", broker, "-t", "test-topic", "-P"] + sasl,
            input=msgs[i].encode(),
            capture_output=True,
            timeout=10
        )
        t1 = time.time()
        if p.returncode != 0:
            print(f"  ERROR on {label} iteration {i}: {p.stderr.decode()[:200]}", file=sys.stderr)
            continue
        times.append((t1 - t0) * 1000)
    return times

print("=== Latency Benchmark (50 iterations each) ===")
print()

print("Benchmarking direct (localhost:9093)...")
direct_times = run_bench("localhost:9093", "direct", 50)

print("Benchmarking proxy (localhost:9092)...")
proxy_times = run_bench("localhost:9092", "proxy", 50)

dm = statistics.mean(direct_times)
dd = statistics.median(direct_times)
ds = statistics.stdev(direct_times)
pm = statistics.mean(proxy_times)
pd = statistics.median(proxy_times)
ps = statistics.stdev(proxy_times)

print()
print(f"Direct:  mean={dm:.3f}ms median={dd:.3f}ms stdev={ds:.3f}ms")
print(f"Proxy:   mean={pm:.3f}ms median={pd:.3f}ms stdev={ps:.3f}ms")

oh_mean = pm - dm
oh_median = pd - dd

print()
print(f"Overhead (mean):   {oh_mean:.3f}ms")
print(f"Overhead (median): {oh_median:.3f}ms")
print()
print(f"Threshold: 2.0ms")
print(f"Mean pass: {'YES' if oh_mean < 2.0 else 'NO'}")
print(f"Median pass: {'YES' if oh_median < 2.0 else 'NO'}")
