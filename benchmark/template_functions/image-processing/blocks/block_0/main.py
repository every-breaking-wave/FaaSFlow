import boto3
import uuid
from time import time
from PIL import Image
from botocore.client import Config
from PIL import Image, ImageFilter
import json
import os

# import oss2
# from oss2.credentials import EnvironmentVariableCredentialsProvider


FILE_NAME_INDEX = 2
TMP = "/tmp/pics/"

FILE_PATH = '/proxy/test_pic.png'

if not os.path.exists(TMP):
    os.makedirs(TMP)

# auth = oss2.ProviderAuth(EnvironmentVariableCredentialsProvider())

# bucket = oss2.Bucket(auth, 'http://oss-cn-shanghai.aliyuncs.com', 'wave-pics')


def flip(image, file_name):
    path_list = []
    name = file_name.split('/')[1]
    #print(name)
    path = TMP + "flip-left-right-"+str(uuid.uuid4()) + name
    img = image.transpose(Image.FLIP_LEFT_RIGHT)
    # img.save(path)
    path_list.append(path)

    path = TMP + "flip-top-bottom-" +str(uuid.uuid4())+ name
    img = image.transpose(Image.FLIP_TOP_BOTTOM)
    # img.save(path)
    path_list.append(path)

    return path_list


def rotate(image, file_name):
    path_list = []
    name = file_name.split('/')[1]
    path = TMP + "rotate-90-" +str(uuid.uuid4())+ name
    img = image.transpose(Image.ROTATE_90)
    # img.save(path)
    path_list.append(path)

    path = TMP + "rotate-180-" +str(uuid.uuid4())+ name
    img = image.transpose(Image.ROTATE_180)
    # img.save(path)
    path_list.append(path)

    path = TMP + "rotate-270-"+str(uuid.uuid4()) + name
    img = image.transpose(Image.ROTATE_270)
    # img.save(path)
    path_list.append(path)

    return path_list


def filter(image, file_name):
    path_list = []
    name = file_name.split('/')[1]
    path = TMP + "blur-" +str(uuid.uuid4()) + name
    img = image.filter(ImageFilter.BLUR)
    # img.save(path)
    path_list.append(path)

    path = TMP + "contour-" +str(uuid.uuid4()) + name
    img = image.filter(ImageFilter.CONTOUR)
    # img.save(path)
    path_list.append(path)

    path = TMP + "sharpen-" +str(uuid.uuid4()) + name
    img = image.filter(ImageFilter.SHARPEN)
    # img.save(path)
    path_list.append(path)

    return path_list


def gray_scale(image, file_name):
    name = file_name.split('/')[1]
    path = TMP + "gray-scale-" +str(uuid.uuid4()) + name
    img = image.convert('L')
    # img.save(path)
    return [path]


def resize(image, file_name):
    name = file_name.split('/')[1]
    path = TMP + "resized-" +str(uuid.uuid4()) + name
    image.thumbnail((128, 128))
    # image.save(path)
    return [path]


def image_processing(file_name, image_path):
    path_list = []
    start = time()
    with Image.open(image_path) as image:
        tmp = image
        path_list += flip(image, file_name)
        path_list += rotate(image, file_name)
        path_list += filter(image, file_name)
        path_list += gray_scale(image, file_name)
        path_list += resize(image, file_name)

    latency = time() - start
    return latency, path_list


def handle(data):
    request_json = json.loads(data)
    # object_name = request_json['object_path']
    request_uuid = request_json['uuid']
    start_time = time()

    # download_path = TMP + '{}.png'.format(uuid.uuid4())
    
    # bucket.get_object_to_file(object_name, download_path)
    # 直接从本地读取
    
    download_time = time() - start_time
    
    latency, path_list = image_processing(FILE_PATH, FILE_PATH)

# infact we don't need to upload the file to oss, we can just return the path_list
    # for upload_path in path_list:
    #     to_upload_path = 'tmp/{}.png'.format(uuid.uuid4())
    #     bucket.put_object_from_file(to_upload_path, upload_path)

    return {
        "statusCode": 200,
        "body": {
            'latency': latency,
            'download_time': download_time,
            'start_time': start_time,
            'uuid': request_uuid,
            'test_name': 'image-processing'
        }
    }

print(handle('{"object_path": "pics/QQ图片20240318201249.png", "uuid": "123"}'))