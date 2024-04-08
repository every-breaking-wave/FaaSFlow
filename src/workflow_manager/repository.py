import gevent

from config import config
import couchdb
import time
couchdb_url = config.COUCHDB_URL
import sys

sys.path.append('../../')
from src.logger.logger import logger

class Repository:
    def __init__(self):
        self.couchdb = couchdb.Server(couchdb_url)
        self.waiting_logs = []

    def create_request_doc(self, request_id: str):
        if request_id in self.couchdb['results']:
            # self.couchdb['results'].delete(self.couchdb['results'][request_id])
            self.couchdb['results'].purge([self.couchdb['results'][request_id]])
        self.couchdb['results'][request_id] = {}

    def clear_couchdb_workflow_latency(self):
        self.couchdb.delete('workflow_latency')
        self.couchdb.create('workflow_latency')

    def clear_couchdb_results(self):
        self.couchdb.delete('results')
        self.couchdb.create('results')

    def save_scalability_config(self, dir_path):
        self.couchdb['results']['scalability_config'] = {'dir': dir_path}

    def save_kafka_config(self, KAFKA_CHUNK_SIZE):
        self.couchdb['results']['kafka_config'] = {'KAFKA_CHUNK_SIZE': KAFKA_CHUNK_SIZE}

    def get_kafka_config(self):
        try:
            return self.couchdb['results']['kafka_config']
        except Exception:
            return None

    def save_latency(self, log):
        self.couchdb['workflow_latency'].save(log)

    def save_redis_log(self, request_id, size, time):
        self.waiting_logs.append({'phase': 'redis', 'request_id': request_id, 'size': size, 'time': time})
        # self.couchdb['workflow_latency'].save({'phase': 'redis', 'request_id': request_id, 'size': size, 'time': time})

    def get_latencies(self, phase):
        requests_logs = {}
        for k in self.couchdb['workflow_latency']:
            doc = self.couchdb['workflow_latency'][k]
            if doc['phase'] == phase:
                request_id = doc['request_id']
                if request_id not in requests_logs:
                    requests_logs[request_id] = []
                requests_logs[request_id].append(doc)
        return requests_logs

    def upload_waiting_logs(self):
        tmp_db = self.couchdb['workflow_latency']
        for log in self.waiting_logs:
            tmp_db.save(log)

    def get_latencies_by_phase_and_workflow_name(self, phase, workflow_name):
        requests_logs = {}
        for k in self.couchdb['workflow_latency']:
            doc = self.couchdb['workflow_latency'][k]
            # template_name的格式是workflow_name__xxx，需要特殊处理一下
            if doc['phase'] == phase and doc['template_name'].startswith(workflow_name):
                # 还需要筛选st在一分钟以内的数据
                if time.time() - doc['st'] > 60:
                    continue
                request_id = doc['request_id']
                if request_id not in requests_logs:
                    requests_logs[request_id] = []
                requests_logs[request_id].append(doc)
        logger.info("get logs size {}".format(len(requests_logs)))
        return requests_logs
    
    def save_workflow_default_runtime(self, workflow_name, runtime):
        # 保存workflow的默认runtime
        # 先读出原有json
        try:
            workflow_info = self.couchdb['workflow_info'][workflow_name]
            workflow_info['default_runtime'] = runtime
            self.couchdb['workflow_info'][workflow_name] = workflow_info
        except Exception:
            self.couchdb['workflow_info'][workflow_name] = {'default_runtime': runtime}
            
    def get_workflow_default_runtime(self, workflow_name):
        try:
            return self.couchdb['workflow_info'][workflow_name]['default_runtime']
        except Exception:
            return None
        

    def save_workflow_code(self, workflow_name, code):
        try:
            workflow_info = self.couchdb['workflow_info'][workflow_name]
            workflow_info['code'] = code
            self.couchdb['workflow_info'][workflow_name] = workflow_info
        except Exception:
            self.couchdb['workflow_info'][workflow_name] = {'code': code}
        
    def get_workflow_code(self, workflow_name):
        try:
            return self.couchdb['workflow_info'][workflow_name]['code']
        except Exception:
            return None