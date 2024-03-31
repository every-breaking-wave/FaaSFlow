import os.path
from typing import Dict

import yaml


# this information is different per workflow
class WorkflowInfo:
    def __init__(self, workflow_name, templates_infos, raw_data):
        self.workflow_name = workflow_name
        self.templates_infos = templates_infos
        self.data = raw_data

    @classmethod
    def parse(cls, config_dict):
        workflows_info = {}
        for path in config_dict.values():
            config_file = os.path.join(path, 'workflow_info.yaml')
            with open(config_file, 'r') as f:
                data = yaml.safe_load(f)
            print(data)
            workflow_name = data['workflow_name']
            # datas_successors = {}
            # functions_predecessors = {}
            templates_infos = {}
            for template_name, template_infos in data['templates'].items():
                templates_infos[template_name] = template_infos
                if template_name == 'VIRTUAL':
                    # Todo: What is for virtual?
                    continue
                for block_name, block_infos in template_infos['blocks'].items():
                    for input_name, input_infos in block_infos['input_datas'].items():
                        if input_infos['type'] == 'NORMAL':
                            pass
                        elif input_infos['type'] == 'LIST':
                            pass
                        else:
                            raise Exception('undefined input type: ', input_infos['type'])
                    for output_name, output_infos in block_infos['output_datas'].items():
                        if output_infos['type'] == 'NORMAL':
                            pass
                        elif output_infos['type'] == 'FOREACH':
                            pass
                        elif output_infos['type'] == 'MERGE':
                            pass
                        else:
                            raise Exception('undefined output type: ', output_infos['type'])

            workflows_info[workflow_name] = cls(workflow_name, templates_infos, data)
        return workflows_info
