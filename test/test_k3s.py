from kubernetes import client
from kubernetes import config as kube_config
import os
import shutil
import time

import requests
import gevent
import string

base_url = "http://{}:{}/{}"
work_dir = "/proxy/mnt"
function_namespace = "function"
default_pod_port = 5000

k3s_kube_config_path = "/etc/rancher/k3s/k3s.yaml"


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
        cls,
        image_name,
        workflow_name,
        blocks_name,
        port,
        attr,
        cpus,
        parallel_limit,
        KAFKA_CHUNK_SIZE,
        runtime_class,
    ) -> "Container":
        # 加载k3s kubeconfig
        # 生成一个随机的字符串作为Pod的名称
        pod_name = (
            image_name
            + "-"
            + "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
        )
        kube_config.load_kube_config(k3s_kube_config_path)
        # TODO: host_path may need to be changed
        host_path = "./"
        host_port = random.randint(30000, 60000)
        pod = client.V1Pod(
            api_version="v1",
            kind="Pod",
            metadata=client.V1ObjectMeta(
                name=pod_name,
                labels={"workflow": "true", "workflow_name": workflow_name},
            ),
            spec=client.V1PodSpec(
                containers=[
                    client.V1Container(
                        name=pod_name,
                        image=image_name,
                        image_pull_policy="IfNotPresent",
                        ports=[
                            client.V1ContainerPort(
                                protocol="TCP", container_port=5000, host_port=int(host_port)
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
                        # security_context=client.V1SecurityContext(
                        #     capabilities=client.V1Capabilities(add=["NET_ADMIN"])
                        # ),
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
                runtime_class_name=runtime_class,
            ),
        )

        # 创建Pod
        api_instance = client.CoreV1Api()
        # 捕捉异常
        try:
            api_response = api_instance.create_namespaced_pod(
                body=pod, namespace=function_namespace
            )
        except Exception as e:
            print("Exception when calling CoreV1Api->create_namespaced_pod: %s\n" % e)
            return None

        # 等待Pod启动...
        while True:
            pod = api_instance.read_namespaced_pod(
                name=pod_name, namespace=function_namespace
            )
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
                r = requests.get(
                    base_url.format(self.pod.status.pod_ip, self.port, "init"),
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

    def destroy(self):
        kube_config.load_kube_config(k3s_kube_config_path)
        api_instance = client.CoreV1Api()
        api_instance.delete_namespaced_pod(
            name=self.pod.metadata.name,
            namespace=function_namespace,
            body=client.V1DeleteOptions(),
        )
        print("Pod deleted.")


import sys
import random
import requests

shim_binary="/usr/bin/containerd-shim-kata-v2"
go_shim_binary = "/opt/kata/bin/containerd-shim-kata-v2"
rust_shim_binary = "opt/kata/runtime-rs/bin/containerd-shim-kata-v2"

if __name__ == "__main__":
    # 用户可以通过输入的参数指定runtimeclass, 获取第一个参数
    runtime_class = sys.argv[1]
    # 如果runtime_class是kata-dragonball，需要切换kata-runtime的softlink到rust版本
    if runtime_class == "kata-dragonball" or runtime_class == "kata":
        os.system("ln -sf {} {}".format(rust_shim_binary, shim_binary))
    else:
        os.system("ln -sf {} {}".format(go_shim_binary, shim_binary))
    
    image_name = "image-processing"
    workflow_name = image_name
    print(f"runtime_class: {runtime_class}")
    # 随机生成一个pod的名字
    pod_name = f"test-pod-{random.randint(0, 1000)}"
    container = Container.create(
        image_name=image_name,
        workflow_name=workflow_name,
        blocks_name=["block1", "block2"],
        port=5000,
        attr="exec",
        cpus=0.2,
        parallel_limit=1,
        KAFKA_CHUNK_SIZE=100000,
        runtime_class=runtime_class,
    )
    
### block_info
# #     blocks:
#       block_0:
#         type: NORMAL
#         input_datas:
#           start: { type: NORMAL }
#         output_datas:
#           img:
#             type: NORMAL
    # data = {
    #     "request_id": "request_1",
    #     "workflow_name": workflow_name,
    #     "template_name": "template_1",
    #     "templates_infos": {
    #         "block1": {"block_name": "block1", "block_inputs": {"input1": "value1"}}
    #     },
    #     "block_name": "block_0",
    #     "block_inputs": {"input1": "value1"},
    #     "chunk_size": 100000,
    #     "block_infos": {
    #         "type": "NORMAL",
    #         "input_datas": {
    #             "start": {"type": "NORMAL"}
    #         },
    #         "output_datas": {
    #             "img": {"type": "NORMAL"}
    #         }
    #     }
    # }
    
    # # 循环发送请求
    # for i in range(10):
    #     r = requests.post(
    #         base_url.format(container.pod.status.pod_ip, default_pod_port, "run_block"),
    #         json=data,
    #     )
    #     duration = r.json()["duration"]
    #     assert r.status_code == 200
    #     print(f"duration: {duration}")
