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
from kubernetes import client
from kubernetes import config as kube_config
import random
import string

base_url = "http://{}:{}/{}"
work_dir = "/proxy/mnt"
namespace = "default"
k3s_kube_config_path = "/etc/rancher/k3s/k3s.yaml"
default_pod_port = 5000

class Container:
    def __init__(
        self, pod, blocks_name, port, attr, parallel_limit, cpu, KAFKA_CHUNK_SIZE
    ):
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
    def create(
        cls, image_name, blocks_name, port, attr, cpus, parallel_limit, KAFKA_CHUNK_SIZE
    ) -> "Container":
        # 加载k3s kubeconfig
        # 生成一个随机的字符串作为Pod的名称
        pod_name = (
            image_name
            + "-"
            + "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
        )
        kube_config.load_kube_config(k3s_kube_config_path)
        host_path = "./"
        pod = client.V1Pod(
            api_version="v1",
            kind="Pod",
            metadata=client.V1ObjectMeta(name=pod_name, labels={"workflow": "true"}),
            spec=client.V1PodSpec(
                containers=[
                    client.V1Container(
                        name=image_name,
                        image=image_name,
                        image_pull_policy="IfNotPresent",
                        ports=[
                            client.V1ContainerPort(
                                protocol="TCP", container_port=5000, host_port=int(port)
                            )
                        ],
                        resources=client.V1ResourceRequirements(
                            limits={"cpu": str(cpus)}
                        ),
                        volume_mounts=[
                            client.V1VolumeMount(
                                mount_path=work_dir, name="work-volume"
                            )
                        ],
                        security_context=client.V1SecurityContext(
                            capabilities=client.V1Capabilities(add=["NET_ADMIN"])
                        ),
                    )
                ],
                volumes=[
                    client.V1Volume(
                        name="work-volume",
                        host_path=client.V1HostPathVolumeSource(
                            path=host_path, type="Directory"
                        ),
                    )
                ],
                runtime_class_name="kata",
            ),
        )

        # 创建Pod
        api_instance = client.CoreV1Api()
        # 捕捉异常
        try:
            api_response = api_instance.create_namespaced_pod(
                body=pod, namespace=namespace
            )
        except Exception as e:
            print("Exception when calling CoreV1Api->create_namespaced_pod: %s\n" % e)
            return None

        # 等待Pod启动...
        while True:
            pod = api_instance.read_namespaced_pod(name=pod_name, namespace=namespace)
            if pod.status.phase == "Running":
                print(f"Pod {pod_name} is running.")
                break
            else:
                time.sleep(0.1)  # 等待一段时间再次检查

        # 初始化
        res = cls(pod, blocks_name, port, attr, parallel_limit, cpus, KAFKA_CHUNK_SIZE)
        res.wait_start()
        return res

    def wait_start(self):
        while True:
            try:
                print(
                    "wait_start, send init request to container ip:",
                    self.pod.status.pod_ip,
                )
                r = requests.get(
                    base_url.format(self.pod.status.pod_ip, default_pod_port, "init"),
                    json={
                        "cpu": self.cpu,
                        "limit_net": True,
                        "KAFKA_CHUNK_SIZE": self.KAFKA_CHUNK_SIZE,
                    },
                )
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
        data = {"datas": datas, "datatype": datatype}
        r = requests.post(
            base_url.format(self.pod.status.pod_ip, default_pod_port, "send_data"), json=data
        )

    # def run_function(self):
    #     # print(data)
    #     r = requests.post(base_url.format(self.port, 'run'))
    #     self.last_time = time.time()
    #     return r.status_code

    def get_prefetch_filepath(self, db_key):
        return os.path.join(config.PREFETCH_POOL_PATH, db_key)

    def check_input_db_data(self, request_id, datainfo, mnt_dir=None):
        if datainfo["datatype"] == "redis_data_ready":
            db_key = datainfo["db_key"]
            if flow_monitor.requests_keys_info[request_id][db_key].in_disk:
                datainfo["datatype"] = "disk_data_ready"
            # else:
            #     flow_monitor.requests_keys_info[request_id][db_key].link_mnts.append(mnt_dir)
        # if datainfo['datatype'] == 'disk_data_ready':
        #     db_key = datainfo['db_key']
        #     shutil.copy(os.path.join(config.PREFETCH_POOL_PATH, db_key), os.path.join(mnt_dir, db_key))

    def link_prefetch_data(
        self,
        request_id,
        workflow_name,
        template_name,
        block_name,
        block_inputs,
        block_infos,
    ):
        # mnt_dir = file_controller.get_container_dir(self.container.id)
        mnt_dir = None

        for name, infos in block_inputs.items():
            datatype = block_infos["input_datas"][name]["type"]
            if datatype == "NORMAL":
                self.check_input_db_data(request_id, infos, mnt_dir)
            elif datatype == "LIST":
                for info in infos.values():
                    self.check_input_db_data(request_id, info, mnt_dir)
            else:
                raise Exception

    def run_gc(self):
        r = requests.post(base_url.format(self.pod.status.pod_ip, default_pod_port, "run_gc"))
        assert r.status_code == 200

    def run_block(
        self,
        request_id,
        workflow_name,
        template_name,
        templates_infos,
        block_name,
        block_inputs,
        block_infos,
    ):
        # this may be redundant, since running_blocks will be added in get_idle_container()
        # self.running_blocks.add(block_name)
        self.link_prefetch_data(
            request_id,
            workflow_name,
            template_name,
            block_name,
            block_inputs,
            block_infos,
        )
        data = {
            "request_id": request_id,
            "workflow_name": workflow_name,
            "template_name": template_name,
            "templates_infos": templates_infos,
            "block_name": block_name,
            "block_inputs": block_inputs,
            "block_infos": block_infos,
            "chunk_size": config.CHUNK_SIZE,
        }
        # print(template_name, block_name, 'container still has idle block?:', self.idle_blocks_cnt)
        print(
            "run block post url",
            base_url.format(self.pod.status.pod_ip, default_pod_port, "run_block"),
            "data",
            data,
        )
        r = requests.post(
            base_url.format(self.pod.status.pod_ip, default_pod_port, "run_block"), json=data
        )
        delay_time = r.json()["delay_time"]
        assert r.status_code == 200
        self.blocks_last_time[block_name] = self.last_time = time.time()
        # self.running_blocks.remove(block_name)
        return delay_time

    def destroy(self):
        kube_config.load_kube_config(k3s_kube_config_path)
        api_instance = client.CoreV1Api()
        api_instance.delete_namespaced_pod(
            name=self.container.metadata.name,
            namespace="default",
            body=client.V1DeleteOptions(),
        )
        print("Pod deleted.")
