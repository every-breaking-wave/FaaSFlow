import os
import sys
import requests

sys.path.append('/home/wave/FaaSFlow/')

from src.workflow_manager.repository import Repository
from config import config

repo = Repository()
gateway_url = 'http://' + config.GATEWAY_URL + '/{}'

def upload_code_to_couchdb(template_name, code_path):
    with open(code_path, 'r') as f:
        code = f.read()
    repo.save_workflow_template_code(template_name, code)
    
def code_analysis(template_name):
    code = repo.get_workflow_template_code(template_name)
    request_info = {'template_name': template_name}
    requests.post(gateway_url.format('code_analyze'), json=request_info)

# Usage: python src/utils/upload_code.py <workflow_name> <code_path>
if __name__ == '__main__':
    # 参数检查
    if len(sys.argv) != 2:
        print('Usage: python src/utils/upload_code.py <workflow_name>')
        exit(1)
    
    print(f'uploading code for workflow {sys.argv[1]}')
    workflow_name = sys.argv[1]
    code_dir = 'benchmark/template_functions'
    # 遍历目录下面所有以workflow_name_开头的文件夹
    for root, dirs, files in os.walk(code_dir):
        for dir in dirs:
            if dir.startswith(f'{workflow_name}'):
                # 实际的代码在dir下面的blocks/block_xxx目录中
                # 获取所有的block_xxx目录
                for root_, dirs_, files_ in os.walk(os.path.join(root, dir, 'blocks')):
                    for dir_ in dirs_:
                        code_path = os.path.join(root_, dir_, 'main.py')
                        print(f'uploading code {code_path}')
                        template_name = dir
                        upload_code_to_couchdb(template_name, code_path)
                        code_analysis(sys.argv[1])