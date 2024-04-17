
stop() {
    ps -ef | grep python3 | grep -v grep | cut -c 9-16 | sudo xargs kill -9 
}

start() {
    stop
    cd /home/wave/FaaSFlow
    mkdir -p ./logs
    cd ./src/workflow_manager

    python3 gateway.py localhost 7000  &

    sleep 5

    sudo python3 test_server.py localhost &

    cd ../../

    # python3 gateway.py localhost 7000 > ../../logs/gateway.log 2>&1 &

    # python3 test_server.py localhost  > ../../logs/test_server.log 2>&1 &

    # 这部分代码必需置于test_prepare_idle之前
    sleep 5
    workflow_name=linpack
    python src/utils/code_utils.py $workflow_name benchmark/template_functions/$workflow_name/blocks/block_0/main.py 


    # 启动测试
    cd test
    
    python3 test_prepare_idle.py



    # 去除了下面代码中的clear，因为test_prepare_idle.py中已经清除了
    # python3 async_test.py > ../logs/test.log 2>&1 &
}

main () {
    # 用户可以输入 start 或者 stop
    if [ $1 == "start" ]; then
        start
    elif [ $1 == "stop" ]; then
        stop
    else
        echo "Usage: start | stop"
    fi
}

main $1