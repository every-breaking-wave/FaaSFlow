from numpy import matrix, linalg, random
from time import time
import json

def linpack(n):
    # LINPACK benchmarks
    time1 = time()
    ops = (2.0 * n) * n * n / 3.0 + (2.0 * n) * n
    
    time2 = time()
    time_delta1 = time2 - time1

    # Create AxA array of random numbers -0.5 to 0.5
    A = random.random_sample((n, n)) - 0.5
    
    time3 = time()
    time_delta2 = time3 - time2
    
    B = A.sum(axis=1)
    time4 = time()
    time_delta3 = time4 - time3
    
    # Convert to matrices
    A = matrix(A)
    B = matrix(B.reshape((n, 1)))
    
    time5 = time()
    time_delta4 = time5 - time4

    # Ax = B
    start = time()
    x = linalg.solve(A, B)
    latency = time() - start

    mflops = (ops * 1e-6 / latency)

    result = {
        'mflops': mflops,
        'latency': latency
    }

    return result

def handle(data):
    request_json = json.loads(data)

    number = int(request_json["number"])
    request_uuid = request_json['uuid']
    start_time = time()

    result = linpack(number)
    return {
        "statusCode": 200,
        "body": {
            **result,
            'start_time': start_time,
            'number': number,
            'uuid': request_uuid,
            'test_name': 'linpack'
        }
    }

print(handle('{"number": 10000, "uuid": "1234"}'))