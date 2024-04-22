
image_name="wordcount"
couchdb_url='http://openwhisk:openwhisk@127.0.0.1:5984/'
workflow_name=$image_name

dir=~/FaaSFlow/benchmark/template_functions/

# 获取当前目录下面以workflow_name开头的文件夹
for d in $dir$workflow_name*; do
    # 获取文件夹名字
    image_name=$(basename $d)
    # 构建docker image
    echo "build $image_name ing..."
    docker build --no-cache -t $image_name $d
    # 获取当前image的id
    image_id=$(docker images -q $image_name)
    # 转为ctr image
    sudo /script/docker-2-ctr.sh $image_id
done


# # 上传代码得到couchdb
cd ~/FaaSFlow/

python3 src/utils/code_utils.py  $workflow_name 