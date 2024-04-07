image_name="image-processing"
couchdb_url='http://openwhisk:openwhisk@127.0.0.1:5984/'
workflow_name=$image_name

docker build -t $image_name ~/FaaSFlow/benchmark/template_functions/$image_name
 # 获取当前image的id
image_id=$(docker images -q $image_name)

# 转为ctr image
sudo /script/docker-2-ctr.sh $image_id


# 1. 上传代码得到couchdb
cd ~/FaaSFlow/

python3 src/utils/code_utils.py  $workflow_name benchmark/template_functions/$image_name/blocks/block_0/main.py