

# 不同runtime对应的平均start latency
runtime_to_start_latency = {
    "runc": 0.5,
    "kata-qemu": 0.8,
    "kata-dragonball": 0.7,
    "kata-fc": 0.6,
}


# 记录不同指标的权重系数，指标包括cpu，memory，start_latency，network，security
cpu_rate_coefficient = 0.5
cpu_total_coefficient = 0.5
memory_coefficient = 0.5
start_latency_coefficient = 0.5
network_coefficient = 0.5
security_coefficient = 0.5
