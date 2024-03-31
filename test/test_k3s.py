from kubernetes import client, config
import os
import shutil
import time
from kubernetes import client, config
import requests
import gevent


base_url = 'http://{}:{}/{}'
work_dir = '/proxy/mnt'
namespace = 'default'

k3s_kube_config_path = '/etc/rancher/k3s/k3s.yaml'
class Pod:
    def __init__(self, pod, blocks_name, port, attr, parallel_limit, cpu, KAFKA_CHUNK_SIZE):
        self.pod = pod
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
        host_path = "./"
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
                    "imagePullPolicy": "IfNotPresent",  # 如果本地没有镜像则拉取
                    "ports": [{
                        "protocol": "TCP",
                        "containerPort": 5000,  # 服务内部端口
                        "targetPort": str(port),  # Pod 容器的端口
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
        # 捕捉异常
        try:
            api_response = api_instance.create_namespaced_pod(body=pod_manifest, namespace=namespace)
        except Exception as e:
            print("Exception when calling CoreV1Api->create_namespaced_pod: %s\n" % e)
            return None

        # 等待Pod启动...        
        while True:
            pod = api_instance.read_namespaced_pod(name=blocks_name, namespace=namespace)
            if pod.status.phase == "Running":
                print(f"Pod {blocks_name} is running.")
                break
            else:
                print(f"Waiting for Pod {blocks_name} to enter Running state. Current state: {pod.status.phase}")
                time.sleep(0.1)  # 等待一段时间再次检查

        # 初始化   
        res= cls(pod, blocks_name, port, attr, parallel_limit, cpus, KAFKA_CHUNK_SIZE)
        res.wait_start()
        return res
    
    def wait_start(self):
        while True:
            try:
                r = requests.get(base_url.format(self.pod.status.pod_ip ,self.port, 'init'), json={'cpu': self.cpu,
                                                                           'limit_net': True,
                                                                           'KAFKA_CHUNK_SIZE': self.KAFKA_CHUNK_SIZE})
                if r.status_code == 200:
                    break
            except Exception:
                pass
            gevent.sleep(0.005)

Pod.create('image-processing', 'test11', 5000, 'attr', 0.2, 'parallel_limit', 'KAFKA_CHUNK_SIZE')