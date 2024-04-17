import numpy as np
import requests
import time
from gevent import monkey
import datetime
import json
import os.path
monkey.patch_all()
import sys
sys.path.append('../')
import requests
import gevent
from src.workflow_manager.repository import Repository
from config import config
import threading

repo = Repository()
gateway_url = 'http://' + config.GATEWAY_URL + '/{}'
slow_threshold = 1000
pre_time = 3 * 60
latencies = []
request_infos = {}
ids = {}



input_args = ''.join(sys.argv[1:])

global test_start
test_start = time.time()


def get_use_container_log(workflow_name, lambd, tests_duration):
    cnt = {}
    avg = {}
    GB_s = 0
    requests_logs = repo.get_latencies('use_container')
    save_logs = {}
    for request_id in ids:
        start_time = ids[request_id]['st']
        duration = ids[request_id]['time']
        slow = False
        if duration > slow_threshold:
            slow = True
        logs = requests_logs[request_id]
        save_logs[request_id] = {}
        save_logs[request_id]['logs'] = []
        save_logs[request_id]['fire_time'] = start_time
        save_logs[request_id]['latency'] = ids[request_id]['latency']
        if slow:
            print(ids[request_id])
        current_request_cnt = {}
        current_request_function_max_log = {}
        for log in logs:
            function_name = log['template_name'] + '_' + log['block_name']
            save_logs[request_id]['logs'].append({'time': log['time'], 'function_name': function_name, 'st': log['st'],
                                                  'ed': log['ed'], 'cpu': log['cpu']})
            GB_s += log['time'] * log['cpu'] * 1280 / 1024
            if function_name not in current_request_cnt:
                current_request_cnt[function_name] = 0
            current_request_cnt[function_name] += 1
            if current_request_cnt[function_name] == 1:
                current_request_function_max_log[function_name] = log
            else:
                if log['time'] > current_request_function_max_log[function_name]['time']:
                    current_request_function_max_log[function_name] = log
            if function_name not in cnt:
                cnt[function_name] = 0
            cnt[function_name] += 1
            if function_name not in avg:
                avg[function_name] = [0, 0, 0]
            avg[function_name][0] += log['time']
            avg[function_name][1] += log['st'] - start_time
            avg[function_name][2] += log['ed'] - start_time
            if slow:
                print(function_name, "%0.3f" % log['time'], "%0.3f" % (log['st'] - start_time),
                      "%0.3f" % (log['ed'] - start_time))

        for func in current_request_cnt:
            if current_request_cnt[func] > 1:
                function_name = func + '_longest'
                log = current_request_function_max_log[func]
                if function_name not in cnt:
                    cnt[function_name] = 0
                cnt[function_name] += 1
                if function_name not in avg:
                    avg[function_name] = [0, 0, 0]
                avg[function_name][0] += log['time']
                avg[function_name][1] += log['st'] - start_time
                avg[function_name][2] += log['ed'] - start_time


    for function_name in cnt:
        print(function_name, end=' ')
        for v in avg[function_name]:
            print("%0.3f" % (v / cnt[function_name]), end=' ')
        print()
    print('Container_GB-s:', format(GB_s / len(latencies), '.3f'))
    nowtime = str(datetime.datetime.now())
    if not os.path.exists('result'):
        os.mkdir('result')
    suffix = 'async_' + workflow_name + '_' + str(lambd) + '_' + str(tests_duration) + f'_({input_args})'

    filepath = os.path.join('result', nowtime + '_' + suffix + '.json')
    with open(filepath, 'w') as f:
        json.dump(save_logs, f)


def cal_percentile():
    percents = [50, 90, 95, 99]
    for percent in percents:
        print(f'P{percent}: ', format(np.percentile(latencies, percent), '.3f'))


def post_request(request_id, workflow_name):
    request_info = {'request_id': request_id,
                    'workflow_name': workflow_name,
                    'input_datas': {'$USER.start': {'datatype': 'entity', 'val': None, 'output_type': 'NORMAL'}}}
    print('--firing--', request_id)
    st = time.time()
    print("request_id", request_id, "workflow_name", workflow_name, "st", st)
    r = requests.post(gateway_url.format('run'), json=request_info)
    ed = time.time()
    print("finish postrequest, ", request_id, ed - st, r.json())
    ids[request_id] = {'time': ed - st, 'st': st, 'ed': ed, 'latency': r.json()['latency']}
    latencies.append(r.json()['latency'])
    print("finish postrequest, ", request_id, ed - st, r.json())



def send_poisson_requests(lambd, num_requests):
    """
    按照泊松分布间隔发送指定数量的HTTP请求。
    :param lambd: 泊松分布的平均事件率（事件/时间单位）
    :param num_requests: 要发送的请求总数
    """
    # 生成num_requests个符合指数分布的间隔时间
    intervals = np.random.exponential(scale=1/lambd, size=num_requests)
    idx = 0
    print("intervals:", intervals)
    threads = []
    for interval in intervals:
        # 按照指数分布的时间间隔等待
        gevent.spawn(post_request, 'request_' + str(idx).rjust(4, '0'), workflow_name)
        time.sleep(interval)
        # t = threading.Thread(target=post_request, args=('request_' + str(idx).rjust(4, '0'), workflow_name, ))
        # threads.append(t)
        # t.start()

    print("finish post all requests")
    gevent.wait()
    print("finish wait")
    time.sleep(5)
    # for t in threads:
    #     t.join(timeout=20)

    print("All requests sent.")
    print('total requests count:', len(latencies))
    get_use_container_log(workflow_name, lambd, duration)
    cal_percentile()


if __name__ == "__main__":
    # 用户可以通过输入的参数指定runtimeclass, 获取第一个参数
    workflow_name = sys.argv[1]
    duration = int(sys.argv[2])  # 单位为s
    lambd = int(sys.argv[3])  # 平均每秒lambd个请求
    num_requests = duration * lambd
    post_request('request_0000', workflow_name)
    # send_poisson_requests(lambd, num_requests)


# 写一条sh命令，删除所有exit的容器
# docker ps -a | grep Exit | awk '{print $1}' | xargs docker rm