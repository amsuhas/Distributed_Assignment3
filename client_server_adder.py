# client.py

import http.client
import json

def send_get_request(host='load_balancer', port=5000, path='/add'):
    connection = http.client.HTTPConnection(host, port)
    payload = {"n": 2, "hostnames": ["server1", "server2"]}
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
    send_get_request()
