
stop() {
    ps -ef | grep python3 | grep -v grep | cut -c 9-16 | sudo xargs kill -9 
}

start() {
    stop
    cd /home/wave/FaaSFlow
    mkdir -p ./logs
    cd ./src/workflow_manager

    python3 gateway.py localhost 7000  &

    python3 test_server.py localhost &

    # python3 gateway.py localhost 7000 > ../../logs/gateway.log 2>&1 &

    # python3 test_server.py localhost  > ../../logs/test_server.log 2>&1 &

    # 启动测试
    cd ../../test
    python3 async_test.py > ../logs/test.log 2>&1 &
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