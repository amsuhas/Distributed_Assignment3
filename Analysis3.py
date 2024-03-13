from codecs import encode, decode
import http.client
import json
import random
import time

def send_post_request_config(payload, host='load_balancer', port=5000, path='/init'):
    print("/init")
    # payload = {
    #     "N":6,
    #     "schema":{"columns":["Stud_id","Stud_name","Stud_marks"],
    #     "dtypes":["Number","String","String"]},
    #     "shards":[{"Stud_id_low":0, "Shard_id": "sh1", "Shard_size":4096},
    #     {"Stud_id_low":4096, "Shard_id": "sh2", "Shard_size":4096},
    #     {"Stud_id_low":8192, "Shard_id": "sh3", "Shard_size":4096},
    #     {"Stud_id_low":12288, "Shard_id": "sh4", "Shard_size":4096}],
    #     "servers":{"Server0":["sh1","sh2"],
    #     "Server1":["sh3","sh4"],
    #     "Server3":["sh1","sh3"],
    #     "Server4":["sh4","sh2"],
    #     "Server5":["sh1","sh4"],
    #     "Server6":["sh3","sh2"]}
    # }
    headers = {'Content-type': 'application/json'}
    json_payload = json.dumps(payload)
    
    connection = http.client.HTTPConnection(host, port)
    connection.request('POST', path, json_payload, headers)

    response = connection.getresponse()
    print(f'Status: {response.status}')
    print('Response:')
    print(response.read().decode('utf-8'))
    print()
    connection.close()

def send_get_request_heartbeat(host='load_balancer', port=5000, path='/heartbeat'):
    print("/heartbeat")
    connection = http.client.HTTPConnection(host, port)
    connection.request('GET', path)

    response = connection.getresponse()
    print(f'Status: {response.status}')
    print('Response:')
    print(response.read().decode('utf-8'))
    print()
    connection.close()

# Define other functions...
def send_get_request_copy(host='load_balancer', port=5000, path='/copy'):
    print("/copy")
    payload = {
        "shards": ["sh1", "sh2"]
    }
    headers = {'Content-type': 'application/json'}
    json_payload = json.dumps(payload)
    
    connection = http.client.HTTPConnection(host, port)
    connection.request('GET', path, json_payload, headers)

    response = connection.getresponse()
    print(f'Status: {response.status}')
    print('Response:')
    response_data = response.read().decode('utf-8')
    json_response = json.loads(response_data)
    # print(json.dumps(json_response, indent=4))
    print(json_response)
    print()
    connection.close()

def send_post_request_read(low, high, host='load_balancer', port=5000, path='/read'):
    print("/read")
    payload = {
        "low":low,
        "high":high
    }
    headers = {'Content-type': 'application/json'}
    json_payload = json.dumps(payload)
    
    connection = http.client.HTTPConnection(host, port)
    connection.request('POST', path, json_payload, headers)

    response = connection.getresponse()
    print(f'Status: {response.status}')
    print('Response:')
    # response_data = decode(encode(response_data, 'latin-1', 'backslashreplace'), 'unicode-escape')
    response_data = response.read().decode('utf-8')
    # print(response_data)
    json_response = json.loads(response_data)
    print(json_response)
    # print(json.dumps(json_response, indent=4))
    print()
    connection.close()

def send_post_request_write(index=0,host='load_balancer', port=5000, path='/write'):
    print("/write")
    payload = {
        "data": [
            {"Stud_id": str(index), "Stud_name": "GHI"+str(index), "Stud_marks": "27"}
            # Add more entries as needed
        ]
    }
    headers = {'Content-type': 'application/json'}
    json_payload = json.dumps(payload)
    
    connection = http.client.HTTPConnection(host, port)
    connection.request('POST', path, json_payload, headers)

    response = connection.getresponse()
    print(f'Status: {response.status}')
    print('Response:')
    response_data = response.read().decode('utf-8')
    json_response = json.loads(response_data)
    print(json.dumps(json_response, indent=4))
    print()
    connection.close()

def send_put_request_update(index=0,host='load_balancer', port=5000, path='/update'):
    print("/update")
    payload = {
        "Stud_id": index,
        "data": {"Stud_id": index, "Stud_name": "GHI", "Stud_marks": "28"}
    }
    headers = {'Content-type': 'application/json'}
    json_payload = json.dumps(payload)
    
    connection = http.client.HTTPConnection(host, port)
    connection.request('PUT', path, json_payload, headers)

    response = connection.getresponse()
    print(f'Status: {response.status}')
    print('Response:')
    response_data = response.read().decode('utf-8')
    json_response = json.loads(response_data)
    print(json.dumps(json_response, indent=4))
    print()
    connection.close()

import http.client
import json

def send_delete_request_del(index=0,host='load_balancer', port=5000, path='/del'):
    print("/del")
    payload = {
        "Stud_id": index
    }
    headers = {'Content-type': 'application/json'}
    json_payload = json.dumps(payload)
    
    connection = http.client.HTTPConnection(host, port)
    connection.request('DELETE', path, json_payload, headers)

    response = connection.getresponse()
    print(f'Status: {response.status}')
    print('Response:')
    response_data = response.read().decode('utf-8')
    json_response = json.loads(response_data)
    print(json.dumps(json_response, indent=4))
    print()
    connection.close()


def send_get_request_status(host='load_balancer', port=5000, path='/status'):
    print("/status")
    connection = http.client.HTTPConnection(host, port)
    connection.request('GET', path)

    response = connection.getresponse()
    print(f'Status: {response.status}')
    print('Response:')
    print(response.read().decode('utf-8'))
    print()
    connection.close()

def send_del_request_rm(host='load_balancer', port=5000, path='/rm'):
    print("/rm")
    payload = {
        "n":1,
        "servers": ["Server0"]
    }
    headers = {'Content-type': 'application/json'}
    json_payload = json.dumps(payload)
    
    connection = http.client.HTTPConnection(host, port,timeout=150)
    connection.request('DELETE', path, json_payload, headers)

    response = connection.getresponse()
    print(f'Status: {response.status}')
    print('Response:')
    response_data = response.read().decode('utf-8')
    json_response = json.loads(response_data)
    print(json.dumps(json_response, indent=4))
    print()
    connection.close()

def send_post_request_add(host='load_balancer', port=5000, path='/add'):
        print("/add")
        payload = {
            "n":2,
            "new_shards": [{"Stud_id_low":12288, "Shard_id": "sh4", "Shard_size":4096}],
            "servers": {
                "Server5":["sh1","sh2","sh4"],
                "Server6":["sh2","sh3","sh4"],
                }
        }
        headers = {'Content-type': 'application/json'}
        json_payload = json.dumps(payload)

        connection = http.client.HTTPConnection(host, port)
        connection.request('POST', path, json_payload, headers)

        response = connection.getresponse()
        print(f'Status: {response.status}')
        print('Response:')
        response_data = response.read().decode('utf-8')
        json_response = json.loads(response_data)
        print(json.dumps(json_response, indent=4))
        print()
        connection.close()

if __name__ == '__main__':
    # send_post_request_config()
    # send_get_request_status()
    # # send_get_request_heartbeat()
    # # send_get_request_copy()
    # send_post_request_write(4)
    # send_post_request_write(2)
    # send_post_request_write(10)
    # send_post_request_write(3)
    # send_post_request_write(19)
    # send_post_request_write(16)
    # send_post_request_write(100)

    # send_post_request_read()
    # send_put_request_update(4)
    # send_post_request_read()

    # send_post_request_add()
    # send_del_request_rm()


    # # send_get_request_copy()
    # send_post_request_read()
    # send_delete_request_del(2)
    # send_post_request_read()
    # # send_get_request_copy()

    random.seed(42)

    servers_count = 10
    shards_count = 6
    shards_replicas = 8
    shards_size = 4096

    # send_post_request_config()
    # send_get_request_status()
    payload = {
        "N":servers_count,
        "schema":{"columns":["Stud_id","Stud_name","Stud_marks"],
        "dtypes":["Number","String","String"]},
        # "shards":[{"Stud_id_low":0, "Shard_id": "sh1", "Shard_size":4096},
        # {"Stud_id_low":4096, "Shard_id": "sh2", "Shard_size":4096},
        # {"Stud_id_low":8192, "Shard_id": "sh3", "Shard_size":4096},
        # {"Stud_id_low":12288, "Shard_id": "sh4", "Shard_size":4096}],
        # "servers":{"Server0":["sh1","sh2"],
        # "Server1":["sh3","sh4"],
        # "Server3":["sh1","sh3"],
        # "Server4":["sh4","sh2"],
        # "Server5":["sh1","sh4"],
        # "Server6":["sh3","sh2"]}
    }

    

    servers = {}
    for i in range(0, shards_count):
        for j in range(0, shards_replicas):
            new_server = "server" + str((i+j)%servers_count)
            if new_server not in servers:
                servers[new_server] = []
            servers[new_server].append("sh"+str(i))


    shards = []
    for i in range(0, shards_count):
        shards.append({"Stud_id_low":i*shards_size, "Shard_id": "sh"+str(i), "Shard_size":shards_size})

    payload["shards"] = shards

    payload["servers"] = servers

    print(payload)

    send_post_request_config(payload)
    send_get_request_status()

    s1 = time.time()

    for i in range(0, 10000):
        send_post_request_write(random.randint(0, shards_count*shards_size - 1))

    s2 = time.time()

    for i in range(0, 10000):
        low = random.randint(0, shards_count*shards_size - 1)
        high = random.randint(low, shards_count*shards_size - 1)
        send_post_request_read(low, high)

    s3 = time.time()

    print("Write time: ", s2-s1)
    print("Read time: ", s3-s2)

