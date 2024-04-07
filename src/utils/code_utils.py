import os
import sys
import requests

sys.path.append('/home/wave/FaaSFlow/')

from src.workflow_manager.repository import Repository
from config import config

repo = Repository()
gateway_url = 'http://' + config.GATEWAY_URL + '/{}'

def upload_code_to_couchdb(workflow_name, code_path):
    with open(code_path, 'r') as f:
        code = f.read()
    repo.save_workflow_code(workflow_name, code)
    
def code_analysis(workflow_name):
    code = repo.get_workflow_code(workflow_name)
    request_info = {'workflow_name': workflow_name}
    requests.post(gateway_url.format('code_analyze'), json=request_info)

# Usage: python src/utils/upload_code.py <workflow_name> <code_path>
if __name__ == '__main__':
    # 参数检查
    if len(sys.argv) != 3:
        print('Usage: python src/utils/upload_code.py <workflow_name> <code_path>')
        exit(1)
        
    upload_code_to_couchdb(sys.argv[1], sys.argv[2])
    code_analysis(sys.argv[1])