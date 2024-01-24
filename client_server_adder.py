# client.py

import http.client
import json
import os
import time

n = int(os.environ.get('NUM_SERVER'))

def send_get_request(host='load_balancer', port=5000, path='/add'):
    connection = http.client.HTTPConnection(host, port)
    payload = {"n": n, "hostnames": []}
    json_data = json.dumps(payload)
    encoded_data = json_data.encode('utf-8')

    headers = {'Content-Type': 'application/json',
           'Content-Length': len(encoded_data)}
    
    connection.request('POST', path, body=encoded_data, headers=headers)
    # connection.request('GET', path)
    response = connection.getresponse()
    print(f'Status: {response.status}')
    print('Response:')
    print(response.read().decode('utf-8'))
    
    connection.close()

if __name__ == '__main__':
    time.sleep(5)
    send_get_request()