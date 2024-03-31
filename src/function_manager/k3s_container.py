import os
import shutil
import time
import requests
import gevent
from docker import DockerClient
from gevent.lock import BoundedSemaphore
from docker.types import Mount
from src.function_manager.file_controller import file_controller
from config import config
from src.workflow_manager.flow_monitor import flow_monitor
from kubernetes import client, config


base_url = 'http://127.0.0.1:{}/{}'
k3s_kube_config_path = '/etc/rancher/k3s/k3s.yaml'
work_dir = '/proxy/mnt'


class Container:
    def __init__(self, container, blocks_name, port, attr, parallel_limit, cpu, KAFKA_CHUNK_SIZE):
        self.container = container
        self.port = port
        self.attr = attr
        self.idle_blocks_cnt = parallel_limit
        self.blocks_last_time = {block_name: time.time() for block_name in blocks_name}
        self.last_time = time.time()
        self.running_blocks = set()
        # self.lock = BoundedSemaphore()
        self.cpu = cpu
        self.KAFKA_CHUNK_SIZE = KAFKA_CHUNK_SIZE

    @classmethod
    def create(cls, image_name, blocks_name, port, attr, cpus, parallel_limit, KAFKA_CHUNK_SIZE) -> 'Container':
        # 加载k3s kubeconfig
        config.load_kube_config(k3s_kube_config_path)
        host_path = config.FILE_CONTROLLER_PATH
        # 设置Pod的规格
        pod_manifest = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": blocks_name,
                "labels": {
                    "workflow": "true"
                }
            },
            "spec": {
                "containers": [{
                    "name": blocks_name,
                    "image": image_name,
                    "ports": [{
                        "protocol": "TCP",
                        "port": 5000,  # 服务内部端口
                        "targetPort": port,  # Pod 容器的端口
                    }],
                    "resources": {
                        "limits": {
                            "cpu": str(cpus),
                            # 添加其他资源限制
                        }
                    },
                    "volumeMounts": [{
                        "mountPath": work_dir,
                        "name": "work-volume"
                    }],
                    "securityContext": {
                        "capabilities": {
                            "add": ["NET_ADMIN"]
                        }
                    }
                }],
                "volumes": [{
                    "name": "work-volume",
                    "hostPath": {
                        "path": host_path,
                        "type": "Directory"
                    }
                }]
            }
        }
        
        # 创建Pod
        api_instance = client.CoreV1Api()
        api_response = api_instance.create_namespaced_pod(body=pod_manifest, namespace='default')
        print("Pod created. status='%s'" % str(api_response.status))
        
        # 等待Pod启动...

        return cls(api_response, blocks_name, port, attr, parallel_limit, cpus, KAFKA_CHUNK_SIZE)


    def wait_start(self):
        while True:
            try:
                r = requests.get(base_url.format(self.port, 'init'), json={'cpu': self.cpu,
                                                                           'limit_net': True,
                                                                           'KAFKA_CHUNK_SIZE': self.KAFKA_CHUNK_SIZE})
                if r.status_code == 200:
                    break
            except Exception:
                pass
            gevent.sleep(0.005)
            

    def wait_start(self):
        while True:
            try:
                r = requests.get(base_url.format(self.port, 'init'), json={'cpu': self.cpu,
                                                                           'limit_net': True,
                                                                           'KAFKA_CHUNK_SIZE': self.KAFKA_CHUNK_SIZE})
                if r.status_code == 200:
                    break
            except Exception:
                pass
            gevent.sleep(0.005)

    # def init(self, request_id, workflow_name, template_name, block_name, block_inputs):
    #     data = {'request_id': request_id,
    #             'workflow_name': workflow_name,
    #             'template_name': template_name,
    #             'block_name': block_name,
    #             'block_inputs': block_inputs}
    #     r = requests.post(base_url.format(self.port, 'init'), json=data)
    #     self.last_time = time.time()
    #     return r.status_code == 200

    def send_data(self, request_id, workflow_name, function_name, datas, datatype):
        # if datatype is BIG, then container's proxy should fetch the big data from couchdb by itself.
        data = {'datas': datas,
                'datatype': datatype}
        r = requests.post(base_url.format(self.port, 'send_data'), json=data)

    # def run_function(self):
    #     # print(data)
    #     r = requests.post(base_url.format(self.port, 'run'))
    #     self.last_time = time.time()
    #     return r.status_code

    def get_prefetch_filepath(self, db_key):
        return os.path.join(config.PREFETCH_POOL_PATH, db_key)

    def check_input_db_data(self, request_id, datainfo, mnt_dir=None):
        if datainfo['datatype'] == 'redis_data_ready':
            db_key = datainfo['db_key']
            if flow_monitor.requests_keys_info[request_id][db_key].in_disk:
                datainfo['datatype'] = 'disk_data_ready'
            # else:
            #     flow_monitor.requests_keys_info[request_id][db_key].link_mnts.append(mnt_dir)
        # if datainfo['datatype'] == 'disk_data_ready':
        #     db_key = datainfo['db_key']
        #     shutil.copy(os.path.join(config.PREFETCH_POOL_PATH, db_key), os.path.join(mnt_dir, db_key))

    def link_prefetch_data(self, request_id, workflow_name, template_name, block_name, block_inputs, block_infos):
        # mnt_dir = file_controller.get_container_dir(self.container.id)
        mnt_dir = None

        for name, infos in block_inputs.items():
            datatype = block_infos['input_datas'][name]['type']
            if datatype == 'NORMAL':
                self.check_input_db_data(request_id, infos, mnt_dir)
            elif datatype == 'LIST':
                for info in infos.values():
                    self.check_input_db_data(request_id, info, mnt_dir)
            else:
                raise Exception

    def run_gc(self):
        r = requests.post(base_url.format(self.port, 'run_gc'))
        assert r.status_code == 200

    def run_block(self, request_id, workflow_name, template_name, templates_infos, block_name, block_inputs,
                  block_infos):
        # this may be redundant, since running_blocks will be added in get_idle_container()
        # self.running_blocks.add(block_name)
        self.link_prefetch_data(request_id, workflow_name, template_name, block_name, block_inputs, block_infos)
        data = {'request_id': request_id,
                'workflow_name': workflow_name,
                'template_name': template_name,
                'templates_infos': templates_infos,
                'block_name': block_name,
                'block_inputs': block_inputs,
                'block_infos': block_infos,
                'chunk_size': config.CHUNK_SIZE}
        # print(template_name, block_name, 'container still has idle block?:', self.idle_blocks_cnt)
        print("run block post url", base_url.format(self.port, 'run_block'), "data", data)
        r = requests.post(base_url.format(self.port, 'run_block'), json=data)
        delay_time = r.json()['delay_time']
        assert r.status_code == 200
        self.blocks_last_time[block_name] = self.last_time = time.time()
        # self.running_blocks.remove(block_name)
        return delay_time

    def destroy(self):
        config.load_kube_config(k3s_kube_config_path)
        api_instance = client.CoreV1Api()
        api_instance.delete_namespaced_pod(name=self.container.metadata.name, namespace='default', body=client.V1DeleteOptions())
        print("Pod deleted.")
