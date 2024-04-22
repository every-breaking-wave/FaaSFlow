

# 不同runtime对应的平均start latency
runtime_to_start_latency = {
    "runc": 0.366,
    "kata-qemu": 1.261,
    "kata-dragonball": 1.955,
    "gvisor": 1.070,
    "kata-fc": 9999,
}


# 记录不同指标的权重系数，指标包括cpu，memory，start_latency，network，security
cpu_rate_coefficient = 0.5
cpu_total_coefficient = 0.5
memory_coefficient = 0.5
start_latency_coefficient = 0.5
network_coefficient = 0.5
security_coefficient = 0.5
