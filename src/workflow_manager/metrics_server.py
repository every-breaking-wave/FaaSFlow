import sys
sys.path.append('../../')

from prometheus_api_client import PrometheusConnect
from prometheus_api_client.utils import parse_datetime
from datetime import datetime, timedelta
from src.workflow_manager.repository import Repository
from src.workflow_manager.constant import runtime_to_start_latency
import gevent

prometheus_server = "http://10.43.138.105:9090"

function_namespace = "function"
dispatch_interval = 10

repo = Repository()


class  MetricsServer:
    def __init__(self):
        self.prom = PrometheusConnect(url=prometheus_server, disable_ssl=True)
        self.cpu_rate_coefficient = 0.5
        self.cpu_total_coefficient = 0.5
        
        
    def init(self):
        gevent.spawn_later(dispatch_interval, self.get_pod_metrics)
    
    def run(self):
        pass
    
    def custom_query_range(self, query, start_time, end_time, step):
        return self.prom.custom_query_range(
            query=query,
            start_time=start_time,
            end_time=end_time,
            step=step
        )
    
    def get_cpu_usage_total_by_name(self, pod_name, namespace):
        # 统计某个workflow在过去1h的cpu使用量
        cpu_query = f'sum(container_cpu_usage_seconds_total{{namespace="{namespace}",pod=~"{pod_name}.*", container!=""}}) by (container)'
        cpu_usage_total_list = self.custom_query_range(cpu_query, parse_datetime("1h"), parse_datetime("now"), '1h')
        print('cpu_usage_total_list:', cpu_usage_total_list)
        cpu_usage_total_list = [x['values'] for x in cpu_usage_total_list][0]
        cpu_usage_total_list = [float(x[1]) for x in cpu_usage_total_list]
        cpu_usage_total = sum(cpu_usage_total_list) / len(cpu_usage_total_list)
        print('cpu_usage_total:', cpu_usage_total)
        return cpu_usage_total
    
    def get_cpu_usage_rate_by_name(self, pod_name, namespace):
        print("pod name is", pod_name)
        cpu_query = f'sum(rate(container_cpu_usage_seconds_total{{namespace="{namespace}",pod=~"{pod_name}.*", container!=""}}[1m])) by (container)'
        cpu_usage_rate_list = self.custom_query_range(cpu_query, parse_datetime("3m"), parse_datetime("now"), '60s')
        # 获取这个dict中的key为'value'的值
        cpu_usage_rate_list = [x['values'] for x in cpu_usage_rate_list][0]
        cpu_usage_rate_list = [float(x[1]) for x in cpu_usage_rate_list]
        cpu_usage_rate = sum(cpu_usage_rate_list) / len(cpu_usage_rate_list)
        print('cpu_usage_rate:', cpu_usage_rate)
        return cpu_usage_rate


    def get_memory_metrics_by_name(self, pod_name, namespace):
        memory_query = f'sum(rate(container_memory_usage_bytes{{namespace="{namespace}",pod=~"{pod_name}.*", container!=""}}[1m])) by (container)'
        
        memory_usage_list = self.custom_query_range(memory_query, parse_datetime("3m"), parse_datetime("now"), '60s')
        print('memory_usage_list:', memory_usage_list)
        memory_usage_list = [x['values'] for x in memory_usage_list][0]
        memory_usage_list = [float(x[1]) for x in memory_usage_list]
        memory_usage = sum(memory_usage_list) / len(memory_usage_list)
        print('memory_usage:', memory_usage)
        return memory_usage
    
    
    def get_pod_metrics(self, namespace):
        gevent.spawn_later(dispatch_interval, self.get_pod_metrics)
        cpu_query = f'sum(rate(container_cpu_usage_seconds_total{{namespace="{namespace}",image!="", container!=""}}[1m])) by (container)'
        memory_query = f'sum(rate(container_memory_usage_bytes{{namespace="{namespace}",image!="", container!=""}}[1m])) by (container)'
        # memory_query = f'container_memory_usage_bytes{{pod="{pod_name}"}}'
    
        # 执行查询
        cpu_data = self.prom.custom_query_range(
            query=cpu_query,
            start_time = parse_datetime("3m"),
            end_time = parse_datetime("now"),
            step='20s',  # 或根据你的需求调整步长
        )
            
        memory_data = self.prom.custom_query_range(
            query=memory_query,
            start_time = parse_datetime("3m"),
            end_time = parse_datetime("now"),
            step='60s',  # 或根据你的需求调整步长
        )
    
        print('cpu_data:', cpu_data)
        print('memory_data:', memory_data)
        
        return cpu_data
        # return cpu_data, memory_data
        
    def calculate_cpu_score(self, cpu_usage_rate, cpu_usage_total):
        # 计算cpu score
        cpu_score =float(cpu_usage_rate) * self.cpu_rate_coefficient + float(cpu_usage_total) * self.cpu_total_coefficient
        return cpu_score
        
    def analyze_workflow(self, workflow_name):
        # 从以下几个方面来分析workflow： cpu， memory， start_latency
        print("analyze workflow {}".format(workflow_name))
        cpu_usage_rate = self.get_cpu_usage_rate_by_name(workflow_name, function_namespace)
        cpu_usage_total = self.get_cpu_usage_total_by_name(workflow_name, function_namespace)
        cpu_score = self.calculate_cpu_score(cpu_usage_rate, cpu_usage_total)
        print("cpu score is", cpu_score)

        memory_data = self.get_memory_metrics_by_name(workflow_name, function_namespace)

        # 获取workflow最近的启动Latency
        start_latency = repo.get_start_latencies(workflow_name)
        # 获取workflow的default runtime
        current_runtime = repo.get_workflow_default_runtime(workflow_name)
        # 对于每一个runtime，我们有一个参照的start latency
        # 这个差值越大，说明这个runtime的启动越慢
        start_latency_diff = start_latency - runtime_to_start_latency[current_runtime]
        print("start latency diff is", start_latency_diff)
        

if __name__ == '__main__':
    metrics_server = MetricsServer()
    metrics_server.analyze_workflow("image-processing")