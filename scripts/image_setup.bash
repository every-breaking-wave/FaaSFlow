#git pull
# docker build --no-cache -t  codeless_base ../src/container
 
docker build  -t  codeless_base ../src/container
#bash ../benchmark/svd/create_image.sh
# bash ../benchmark/video/create_image.sh
# bash ../benchmark/wordcount/create_image.sh
# bash ../benchmark/recognizer/create_image.sh
bash ../benchmark/linpack/create_image.sh
bash ../benchmark/image-processing/create_image.sh
docker image prune -f
