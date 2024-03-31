import subprocess

def run_container_with_runtime(image, container_id, runtime):
    # 构建ctr命令
    command = [
        "ctr", "run", 
        "--runtime", runtime, 
        image, container_id
    ]
    
    # 执行ctr命令
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"Container {container_id} started successfully")
    else:
        raise Exception(f"Failed to start container: {result.stderr}")

# 示例用法
image = "docker.io/library/busybox:latest"  # 使用busybox镜像作为示例
container_id = "example_busybox"  # 容器ID
runtime = "io.containerd.run.kata.v2"  # 指定为kata-runtime，根据实际配置的runtime类型更改

run_container_with_runtime(image, container_id, runtime)
