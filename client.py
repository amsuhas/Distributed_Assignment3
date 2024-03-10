from codecs import encode, decode
import http.client
import json

def send_post_request_config(host='dummy', port=5000, path='/config'):
    print("/config")
    payload = {
        "schema": {"columns": ["Stud_id", "Stud_name", "Stud_marks"], "dtypes": ["Number", "String", "String"]},
        "shards": ["sh1", "sh2"]
    }
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

def send_get_request_heartbeat(host='dummy', port=5000, path='/heartbeat'):
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
def send_get_request_copy(host='dummy', port=5000, path='/copy'):
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

def send_post_request_read(host='dummy', port=5000, path='/read'):
    print("/read")
    payload = {
        "shard": "sh2",
        "Stud_id": {"low": 2, "high": 5}
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

def send_post_request_write(index=0,host='dummy', port=5000, path='/write'):
    print("/write")
    payload = {
        "shard": "sh2",
        "curr_idx": str(index),
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

def send_put_request_update(index=0,host='dummy', port=5000, path='/update'):
    print("/update")
    payload = {
        "shard": "sh2",
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

def send_delete_request_del(index=0,host='dummy', port=5000, path='/del'):
    print("/del")
    payload = {
        "shard": "sh2",
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

if __name__ == '__main__':
    # send_post_request_config()
    send_get_request_heartbeat()
    send_get_request_copy()
    send_post_request_write(4)
    send_post_request_write(2)
    send_post_request_write(10)
    send_post_request_write(3)
    send_post_request_write(19)
    send_post_request_write(16)
    send_post_request_write(100)

    send_post_request_read()
    send_put_request_update(0)
    send_post_request_read()

    send_get_request_copy()
    send_post_request_read()
    send_delete_request_del(0)
    send_post_request_read()
    send_get_request_copy()


