
from couchdb import Server
import time

time.sleep(2)
db = Server('http://openwhisk:openwhisk@localhost:5984')
# db.delete('workflow_latency')
db.create('_users')
db.create('workflow_info')
db.create('workflow_latency')
# db.delete('results')
db.create('results')
# db.delete('log')
db.create('log')
