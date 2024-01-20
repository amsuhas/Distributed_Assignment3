import http.client
import time
import asyncio
import random

def send_get_request(host='localhost', port=8000, path='/'):
    connection = http.client.HTTPConnection(host, port)
    connection.request('GET', path)
    
    response = connection.getresponse()
    # print(f'Status: {response.status}')
    # print('Response:')
    # print(response.read().decode('utf-8'))
    
    connection.close()
    return response


def send_get_request_with_timeout(host='localhost', port=8000, path='/heartbeat', timeout=5):
    connection = http.client.HTTPConnection(host, port, timeout=timeout)
    
    try:
        connection.request('GET', path)
        response = connection.getresponse()
        
        pass
    except http.client.HTTPException as e:
        with shared_data.mutex:
            shared_data.rm_server(host)
        
    finally:
        connection.close()


serv_list = []


# concurrent_server_with_mutex.py

from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
import threading
import json
import math



class SharedData:
    def __init__(self):
        self.num_serv = 0
        self.counter = 0
        self.serv_dict = {}
        self.buf_size = 512
        self.num_vservs = int(math.log2(self.buf_size))
        self.cont_hash = [[None, None] for _ in range(self.buf_size)]
        self.serv_id_dict = {}
        self.mutex = threading.Lock()


    def get_hash(self,host_name):
        li=[]
        for j in range(self.num_vservs):
            nindex=(self.counter*self.counter + j*j + 2*j + 25)
            nindex %= self.buf_size
            jp=0
            while(jp<self.buf_size):
                if self.cont_hash[nindex][0]!=None:
                    nindex+=1
                    nindex%=self.buf_size
                else:
                    self.serv_id_dict[nindex]=None
                    self.cont_hash[nindex][0]=host_name
                    li.append(nindex)
                    break
                jp+=1

        return li

    def client_hash(self,r_id):
        nindex = r_id*r_id + 2*r_id + 17
        nindex %= self.buf_size
        jp=0
        while(jp<self.buf_size):
            if self.cont_hash[nindex][1]!=None:
                nindex+=1
                nindex%=self.buf_size
            else:
                self.cont_hash[nindex][1]=r_id
                break
            jp+=1
        n_index += 1
        n_index %= self.buf_size
        if(len(self.serv_id_dict) != 0):
            lower_bound_key = self.serv_id_dict.bisect_left(n_index)
            if lower_bound_key == len(self.serv_id_dict):
                return self.cont_hash[self.serv_id_dict[0]][0]
            else:
                self.cont_hash[self.serv_id_dict.iloc[lower_bound_key]][0]

        else:
            return None
        

    def rm_server(self,host_name):
        indexes=self.serv_dict[host_name]
        for ind in indexes:
            self.cont_hash[0]=None
            del self.serv_id_dict[host_name]
        self.num_serv-=1
        del self.serv_dict[host_name]





shared_data = SharedData()

# Extend SimpleHTTPRequestHandler to use shared data
class SimpleHandlerWithMutex(SimpleHTTPRequestHandler):

    def do_POST(self):
        if(self.path == '/add'):
            content_length = int(self.headers['Content-Length'])
            content = self.rfile.read(content_length).decode('utf-8')
            num_servs = int(content["n"])
            name_servs = content["replicas"]
            if(len(name_servs) > num_servs):
                self.send_response(400)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                server_response = {"message": "<Error> Length of hostname list is more than newly added instances", "status": "failure"}
                response_str = json.dumps(server_response)
                self.wfile.write(response_str.encode('utf-8'))
                return
            with self.shared_data.mutex:
                for i in range(num_servs):
                    self.shared_data.num_serv += 1
                    self.shared_data.counter += 1
                    if(i>=len(name_servs)):
                        new_name = "server" + self.shared_data.counter
                    else:
                        if(name_servs[i] not in self.shared_data.serv_dict):
                            new_name = name_servs[i]
                        else:
                            new_name = "server" + self.shared_data.counter

                    
                    if self.shared_data.buf_size - self.shared_data.num_vservs*self.shared_data.num_serv <self.shared_data.num_vservs:
                        self.shared_data.counter-=1
                        self.shared_data.num_serv-=1
                        self.send_response(400)
                        self.send_header('Content-type', 'application/json')
                        self.end_headers()
                        server_response = {"message": "<Error> Length of hostname list is more than newly added instances", "status": "failure"}
                        response_str = json.dumps(server_response)
                        self.wfile.write(response_str.encode('utf-8'))
                        return
                    serv_listid = self.shared_data.get_hash(new_name)
                    self.shared_data.serv_dict[new_name] = serv_listid

            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            server_response = {"message": {"N": self.shared_data.num_serv, "replicas": [hostname for hostname in self.shared_data.serv_dict.keys()]}, "status": "successful"}
            response_str = json.dumps(server_response)
            self.wfile.write(response_str.encode('utf-8'))
            return
        
    def do_DELETE(self):
        if(self.path == '/rm'):
            content_length = int(self.headers['Content-Length'])
            content = self.rfile.read(content_length).decode('utf-8')
            num_servs = int(content["n"])
            name_servs = content["replicas"]
            if(len(name_servs) > num_servs):
                self.send_response(400)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                server_response = {"message": "<Error> Length of hostname list is more than newly added instances", "status": "failure"}
                response_str = json.dumps(server_response)
                self.wfile.write(response_str.encode('utf-8'))
                return
            with self.shared_data.mutex:
                for i in range(num_servs):
                    # self.shared_data.num_serv += 1
                    # self.shared_data.counter += 1
                    if(i>=len(name_servs)):
                        first_key, first_value = next(iter(self.shared_data.serv_dict.items()))
                        self.shared_data.rm_server(first_key)
                            
                    else:
                        if(name_servs[i] not in self.shared_data.serv_dict):
                            self.shared_data.counter-=1
                            self.shared_data.num_serv-=1
                            self.send_response(400)
                            self.send_header('Content-type', 'application/json')
                            self.end_headers()
                            server_response = {"message": "<Error> Server not found", "status": "failure"}
                            response_str = json.dumps(server_response)
                            self.wfile.write(response_str.encode('utf-8'))
                            return
                        else:
                            self.shared_data.rm_server(name_servs[i])

            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            server_response = {"message": {"N": self.shared_data.num_serv, "replicas": [hostname for hostname in self.shared_data.serv_dict.keys()]}, "status": "successful"}
            response_str = json.dumps(server_response)
            self.wfile.write(response_str.encode('utf-8'))
            return


    def do_GET(self):
        with self.shared_data.mutex:
            self.shared_data.counter += 1
            counter_value = self.shared_data.counter

        if(self.path == '/rep'):
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            server_response = {"message": {"N": self.shared_data.num_serv, "replicas": [hostname for hostname in self.shared_data.serv_dict.keys()]}, "status": "successful"}
            response_str = json.dumps(server_response)
            self.wfile.write(response_str.encode('utf-8'))
            return
        else:
            rid = random.randrange(1, 1000000, 1)
            while(1):
                serv_id = self.shared_data.client_hash(rid)
                if serv_id == None:
                    continue
                else:
                    response = send_get_request(serv_id, 8001, self.path)
                    self.send_response(response.status)
                    self.send_header('Content-type', response.getheader('Content-type'))
                    self.end_headers()
                    self.wfile.write(response.read())
                    return


if __name__ == '__main__':
    server_address = ('', 8000)
    httpd = ThreadingHTTPServer(server_address, SimpleHandlerWithMutex)
    
    print('Starting server on port 8000...')
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print('Server is shutting down...')
        httpd.shutdown()
    
    while(1):
        for host_name in shared_data.serv_dict.keys():
            send_get_request_with_timeout(host_name)
        time.sleep(5)

# async
