# client.py

import http.client
import json
import time

def send_get_request_rep(host='load_balancer', port=5000, path='/rep'):
    print("/rep")
    connection = http.client.HTTPConnection(host, port)
    connection.request('GET', path)

    response = connection.getresponse()
    print(f'Status: {response.status}')
    print('Response:')
    print(response.read().decode('utf-8'))
    print()
    connection.close()


def send_get_request_add(n=1, hostnames=[], host='load_balancer', port=5000, path='/add'):
    print("/add")
    connection = http.client.HTTPConnection(host, port)
    payload = {"n": n, "hostnames": hostnames}
    json_data = json.dumps(payload)
    encoded_data = json_data.encode('utf-8')
    headers = {'Content-Type': 'application/json', 'Content-Length': len(encoded_data)}
    connection.request('POST', path, body=encoded_data, headers=headers)

    response = connection.getresponse()
    print(f'Status: {response.status}')
    print('Response:')
    print(response.read().decode('utf-8'))
    print()
    connection.close()


def send_get_request_rm(n=1, hostnames=[], host='load_balancer', port=5000, path='/rm'):
    print("/rm")
    connection = http.client.HTTPConnection(host, port)
    payload = {"n": n, "hostnames": hostnames}
    json_data = json.dumps(payload)
    encoded_data = json_data.encode('utf-8')
    headers = {'Content-Type': 'application/json', 'Content-Length': len(encoded_data)}
    connection.request('DELETE', path, body=encoded_data, headers=headers)

    response = connection.getresponse()
    print(f'Status: {response.status}')
    print('Response:')
    print(response.read().decode('utf-8'))
    print()
    connection.close()


def send_get_request_home(host='load_balancer', port=5000, path='/home'):
    print("/home")
    connection = http.client.HTTPConnection(host, port)
    connection.request('GET', path)
    
    response = connection.getresponse()
    print(f'Status: {response.status}')
    print('Response:')
    print(response.read().decode('utf-8'))
    print()
    connection.close()




if __name__ == '__main__':
    send_get_request_rep()
    time.sleep(2)
    send_get_request_add()
    time.sleep(2)
    send_get_request_home()
    time.sleep(2)
    send_get_request_add(2,["serv-a"])
    time.sleep(2)
    send_get_request_home()
    time.sleep(2)
    send_get_request_add(1,["A","B","C"])
    time.sleep(2)
    send_get_request_rm()
    time.sleep(2)
    send_get_request_home()
    time.sleep(2)
    send_get_request_rm(2,["x","y"])
    time.sleep(2)
    send_get_request_rep()

