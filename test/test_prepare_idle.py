

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

workflow_name = 'image-processing'
replicas = 20


def clean_worker(addr):
    r = requests.post(f'http://{addr}:7999/clear')
    assert r.status_code == 200
    

def post_request(request_id, workflow_name, replicas):
    request_info = {'request_id': request_id,
                    'workflow_name': workflow_name,
                    'replicas': 20,
                }

    # print('--firing--', request_id)
    st = time.time()
    print("request_id", request_id, "workflow_name", workflow_name, "st", st)
    r = requests.post(gateway_url.format('prepare_container'), json=request_info)


threads_ = []
for addr in config.WORKER_ADDRS:
    t = threading.Thread(target=clean_worker, args=(addr, ))
    threads_.append(t)
    t.start()

for t in threads_:
    t.join()

post_request('request_' + '0', workflow_name, 10)
