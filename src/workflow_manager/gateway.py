from gevent import monkey

monkey.patch_all()
import time

from gevent import event
import sys

sys.path.append('../../')
import json
import gevent
import requests
import couchdb
import threading

from typing import Dict
from config import config
from repository import Repository
from flask import Flask, request
from gevent.pywsgi import WSGIServer
from workflow_info import WorkflowInfo
from kafka import KafkaAdminClient
from src.logger.logger import logger

app = Flask(__name__)
repo = Repository()
workflows_info = WorkflowInfo.parse(config.WORKFLOWS_INFO_PATH)
worker_addrs = config.WORKER_ADDRS

lock = threading.Lock()
TIME_WINDOW = 60
THRESHOLD = 2

class RequestInfo:
    def __init__(self, request_id):
        self.request_id = request_id
        self.result = event.AsyncResult()


requests_info: Dict[str, RequestInfo] = {}
workersp_url = 'http://{}:8000/{}'


def analyze_workflow(workflow_name: str):
    logger.info("analyze workflow {}".format(workflow_name))
    workflow_info = workflows_info[workflow_name]
    # 从couchdb中获取workflow的最近的results和workflow_latency信息
    request_logs = repo.get_latencies_by_phase_and_workflow_name('use_container', workflow_name)
    
    
    
@app.route('/run', methods=['POST'])
def run():
    inp = request.get_json(force=True, silent=True)
    workflow_name = inp['workflow_name']
    request_id = inp['request_id']
    input_datas = inp['input_datas']
    # repo.create_request_doc(request_id)
    requests_info[request_id] = RequestInfo(request_id)
    print("workflows info is", workflows_info)
    workflow_info = workflows_info[workflow_name]
    if time.time() - workflow_info.timestamp > TIME_WINDOW:
        workflow_info.cnt = 1
    else:
        workflow_info.cnt += 1
    workflow_info.timestamp = time.time()
    if workflow_info.cnt > THRESHOLD:
        threading.Thread(target=analyze_workflow, args=(workflow_name, )).start()
    worker_num = len(worker_addrs)
    templates_info = {}
    print("workflow_info.templates_infos", workflow_info.templates_infos)
    for i, template_name in enumerate(workflow_info.templates_infos):
        ip = worker_addrs[i % worker_num]
        print(template_name, ip)
        templates_info[template_name] = {'ip': ip}
    # templates_info = {template_name: {'ip': '127.0.0.1'} for template_name in workflow_info.templates_infos}
    # split video workflow to different nodes!
    # print(templates_info)
    data = {'request_id': request_id,
            'workflow_name': workflow_name,
            'templates_info': templates_info}
    ips = set()
    for template_info in templates_info.values():
        ips.add(template_info['ip'])
    st = time.time()
    # 1. transmit request_info to all relative nodes
    events = []
    for ip in ips:
        remote_url = workersp_url.format(ip, 'request_info')
        # headers = {'Connection': 'close'}
        events.append(gevent.spawn(requests.post, remote_url, json=data))
        print("add event", remote_url)
    gevent.joinall(events)
    # 2. transmit input_datas of this request to relative nodes
    # 2.1 gather input_datas of each IP
    ips_datas_mapping: Dict[str, dict] = {}
    for input_data_name in input_datas:
        for dest_template_name in workflow_info.data['global_inputs'][input_data_name]['dest']:
            ip = templates_info[dest_template_name]['ip']
            if ip not in ips_datas_mapping:
                ips_datas_mapping[ip] = {}
            if input_data_name not in ips_datas_mapping[ip]:
                ips_datas_mapping[ip][input_data_name] = input_datas[input_data_name]
    # 2.2 transmit input_datas
    # Todo. Assume user's input_datas are small.
    events = []
    for ip in ips_datas_mapping:
        remote_url = workersp_url.format(ip, 'transfer_data')
        data = {'request_id': request_id,
                'workflow_name': workflow_name,
                'template_name': 'global_inputs',
                'block_name': 'global_inputs',
                'datas': ips_datas_mapping[ip]}
        # headers = {'Connection': 'close'}
        events.append(gevent.spawn(requests.post, remote_url, json=data))
        print("add event", remote_url)
    gevent.joinall(events)
    result = requests_info[request_id].result.get()
    ed = time.time()
    return json.dumps({'result': result, 'latency': ed - st})


@app.route('/code_analyze', methods=['POST'])
def analyze_func_code():
    inp = request.get_json(force=True, silent=True)
    code = repo.get_workflow_code(inp['workflow_name'])
    # prompt = 'Analyze the following code and provide feedback:' + \
    #     'you should only consider whether it is cpu or memory intensive, whether it is io intensive, and whether it is network intensive.' + \
    #     'and whether it is secure sensitive, you should reply with a list of the above four items, each with a value of "yes" or "no".' + \
    #     'at the end, you should give one appropriate runtime for this code, you can only choose from [kata-qemu, kata-fc, kata-dragonball, runc]' + \
    #     'code is :\n' + code + '\n'
    # client = openai.Client()
    # stream = client.chat.completions.create(
    #     model="gpt-4",
    #     messages=[
    #         {"role": "user", "content": prompt},
    #     ],
    #     stream=True
    # )
    
    # # 期待的返回值是一个list，包含四个元素，每个元素是yes或者no，最后给出的是推荐的runtime string
    # func_features = []
    # for chunk in stream:
    #     if chunk.choices[0].delta.content is not None:
    #         func_features.append(chunk.choices[0].delta.content)
    
    # llm会返回一个list，包含五个元素，每个元素是yes或者no，最后给出的是推荐的runtime string
    # 前五个依次是cpu, memory, io, network，security， 若返回值为yes则说明该项敏感，否则不敏感
    # 最后一个是runtime，可以选择[kata-qemu, kata-fc, kata-dragonball, runc]
    # 先mock code_analyze的返回值为[yes, no, yes, no, yes, runc] 
    llm_response = ['yes', 'no', 'yes', 'no', 'yes', 'runc']
    # request信息中包含workflow的名字，从而将workflow:runtime的信息更新到couchdb中
    repo.save_workflow_default_runtime(inp['workflow_name'], llm_response[-1])
    return 'OK', 200
    

@app.route('/post_user_data', methods=['POST'])
def post_user_data():
    inp = request.get_json(force=True, silent=True)
    request_id = inp['request_id']
    datas = inp['datas']
    # print(datas)
    requests_info[request_id].result.set(datas)
    return 'OK', 200


@app.route('/clear', methods=['POST'])
def clear():
    # client.delete_topics(client.list_topics())
    requests_info.clear()
    return 'OK', 200


if __name__ == '__main__':
    # print("connect to kafka at {}".format(config.KAFKA_URL))
    # client = KafkaAdminClient(bootstrap_servers=config.KAFKA_URL)
    # client.delete_topics(client.list_topics())
    server = WSGIServer((sys.argv[1], int(sys.argv[2])), app)
    server.serve_forever()
