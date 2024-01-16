# client.py

import http.client

def send_get_request(host='localhost', port=8000, path='/'):
    connection = http.client.HTTPConnection(host, port)
    connection.request('GET', path)
    
    response = connection.getresponse()
    print(f'Status: {response.status}')
    print('Response:')
    print(response.read().decode('utf-8'))
    
    connection.close()

if __name__ == '__main__':
    send_get_request()
