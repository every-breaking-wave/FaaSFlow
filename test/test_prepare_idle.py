

from gevent import monkey

monkey.patch_all()
import gevent
import sys
import time

sys.path.append('../')
import requests
from config import config
import threading


gateway_url = 'http://' + config.GATEWAY_URL + '/{}'

# workflow_name = 'linpack'
workflow_name = 'wordcount'
replicas = 20


def clean_worker(addr):
    r = requests.post(f'http://{addr}:7999/clear')
    assert r.status_code == 200
    

def post_request(request_id, workflow_name, runtime_class_name, replicas):
    request_info = {'request_id': request_id,
                    'workflow_name': workflow_name,
                    'runtime_class_name': runtime_class_name,
                    'replicas': int(replicas),
                }

    # print('--firing--', request_id)
    st = time.time()
    print("request_id", request_id, "workflow_name", workflow_name, "runtime_class_name", runtime_class_name, "replicas", int(replicas))
    r = requests.post(gateway_url.format('prepare_idle_container'), json=request_info)




if __name__ == '__main__':
    workflow_name = sys.argv[1]
    runtime_class_name = sys.argv[2]
    replicas = sys.argv[3]

    runtime_class_name = ""
    
    threads_ = []
    for addr in config.WORKER_ADDRS:
        t = threading.Thread(target=clean_worker, args=(addr, ))
        threads_.append(t)
        t.start()

    for t in threads_:
        t.join()

    post_request('request_' + '0', workflow_name, runtime_class_name, replicas)

