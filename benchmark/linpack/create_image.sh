image_name="linpack"
workflow_name = "linpack"

docker build -t $image_name ~/FaaSFlow/benchmark/template_functions/$image_name
 # 获取当前image的id
image_id=$(docker images -q $image_name)

# 转为ctr image
sudo /script/docker-2-ctr.sh $image_id

cd ~/FaaSFlow/

python3 src/utils/code_utils.py  $workflow_name benchmark/template_functions/$image_name/blocks/block_0/main.py