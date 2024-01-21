import http.client
import time
import asyncio
import random
from sortedcontainers import SortedDict
import docker
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
import threading
import json
import math


# ports = {"server1": 8001, "server2": 8002}
def send_get_request(host='localhost', port=5000, path='/'):
    connection = http.client.HTTPConnection(host, port)
    connection.request('GET', path)
    
    response = connection.getresponse()
    # print(f'Status: {response.status}')
    # print('Response:')
    # print(response.read().decode('utf-8'))
    
    connection.close()
    return response


def send_get_request_with_timeout(host='localhost', port=5000, path='/heartbeat', timeout=0.5):
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




class SharedData:
    def __init__(self):
        self.num_serv = 0
        self.counter = 0
        self.serv_dict = {}
        self.buf_size = 512
        self.num_vservs = int(math.log2(self.buf_size))
        self.cont_hash = [[None, None] for _ in range(self.buf_size)]
        self.serv_id_dict = SortedDict()
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
                    self.serv_id_dict[nindex]= None
                    self.cont_hash[nindex][0]=host_name
                    li.append(nindex)
                    break
                jp+=1
        print(li)
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
        nindex += 1
        nindex %= self.buf_size
        print(self.serv_id_dict)
        if(len(self.serv_id_dict) != 0):
            lower_bound_key = self.serv_id_dict.bisect_left(nindex)
            if lower_bound_key == len(self.serv_id_dict):
                return self.cont_hash[self.serv_id_dict.iloc[0]][0]
            else:
                return self.cont_hash[self.serv_id_dict.iloc[lower_bound_key]][0]

        else:
            return None
        

    def rm_server(self,host_name):
        indexes=self.serv_dict[host_name][0]
        for ind in indexes:
            self.cont_hash[0]=None
            del self.serv_id_dict[ind]
        self.num_serv-=1
        container = self.serv_dict[host_name][1]
        del self.serv_dict[host_name]
        time.sleep(5)
        container.stop()
        container.remove()




client = docker.from_env()
shared_data = SharedData()

# Extend SimpleHTTPRequestHandler to use shared data
class SimpleHandlerWithMutex(SimpleHTTPRequestHandler):

    def do_POST(self):
        if(self.path == '/add'):
            content_length = int(self.headers['Content-Length'])
            content = self.rfile.read(content_length).decode('utf-8')
            content = json.loads(content)
            num_servs = int(content["n"])
            name_servs = content["hostnames"]
            if(len(name_servs) > num_servs):
                self.send_response(400)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                server_response = {"message": "<Error> Length of hostname list is more than newly added instances", "status": "failure"}
                response_str = json.dumps(server_response)
                self.wfile.write(response_str.encode('utf-8'))
                return
            with shared_data.mutex:
                for i in range(num_servs):
                    shared_data.num_serv += 1
                    shared_data.counter += 1
                    if(i>=len(name_servs)):
                        new_name = "server" + shared_data.counter
                    else:
                        if(name_servs[i] not in shared_data.serv_dict):
                            new_name = name_servs[i]
                        else:
                            new_name = "server" + shared_data.counter

                    
                    if shared_data.buf_size - shared_data.num_vservs*shared_data.num_serv <shared_data.num_vservs:
                        shared_data.counter-=1
                        shared_data.num_serv-=1
                        self.send_response(400)
                        self.send_header('Content-type', 'application/json')
                        self.end_headers()
                        server_response = {"message": "<Error> Length of hostname list is more than newly added instances", "status": "failure"}
                        response_str = json.dumps(server_response)
                        self.wfile.write(response_str.encode('utf-8'))
                        return
                    serv_listid = shared_data.get_hash(new_name)
                    container = client.containers.run("server_image", detach=True, hostname = new_name, name = new_name, network_mode='bridge')
                    shared_data.serv_dict[new_name] = [serv_listid,container]
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            server_response = {"message": {"N": shared_data.num_serv, "replicas": [hostname for hostname in shared_data.serv_dict.keys()]}, "status": "successful"}
            response_str = json.dumps(server_response)
            self.wfile.write(response_str.encode('utf-8'))
            return
        
    def do_DELETE(self):
        if(self.path == '/rm'):
            content_length = int(self.headers['Content-Length'])
            content = self.rfile.read(content_length).decode('utf-8')
            content = json.loads(content)
            num_servs = int(content["n"])
            name_servs = content["hostnames"]
            if(len(name_servs) > num_servs):
                self.send_response(400)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                server_response = {"message": "<Error> Length of hostname list is more than newly added instances", "status": "failure"}
                response_str = json.dumps(server_response)
                self.wfile.write(response_str.encode('utf-8'))
                return
            with shared_data.mutex:
                for i in range(len(name_servs)):
                    if(name_servs[i] not in shared_data.serv_dict):
                        self.send_response(400)
                        self.send_header('Content-type', 'application/json')
                        self.end_headers()
                        server_response = {"message": "<Error> Server not found", "status": "failure"}
                        response_str = json.dumps(server_response)
                        self.wfile.write(response_str.encode('utf-8'))
                        return
                for i in range(num_servs):
                    if(i>=len(name_servs)):
                        first_key, first_value = next(iter(shared_data.serv_dict.items()))
                        shared_data.rm_server(first_key)
                    else:
                        shared_data.rm_server(name_servs[i])
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            server_response = {"message": {"N": shared_data.num_serv, "replicas": [hostname for hostname in shared_data.serv_dict.keys()]}, "status": "successful"}
            response_str = json.dumps(server_response)
            self.wfile.write(response_str.encode('utf-8'))
            return

    def do_GET(self):
        with shared_data.mutex:
            shared_data.counter += 1
            counter_value = shared_data.counter

        if(self.path == '/rep'):
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            server_response = {"message": {"N": shared_data.num_serv, "replicas": [hostname for hostname in shared_data.serv_dict.keys()]}, "status": "successful"}
            response_str = json.dumps(server_response)
            self.wfile.write(response_str.encode('utf-8'))
            return
        else:
            rid = random.randrange(99999, 1000000, 1)
            while(1):
                serv_id = shared_data.client_hash(rid)
                if serv_id == None:
                    continue
                else:
                    # port = ports[serv_id]
                    response = send_get_request(serv_id, 5000, self.path)
                    # response = send_get_request('localhost', port, self.path)
                    self.send_response(response.status)
                    self.send_header('Content-type', response.getheader('Content-type'))
                    self.end_headers()
                    self.wfile.write(response.read())
                    return


if __name__ == '__main__':
    server_address = ('', 5000)
    httpd = ThreadingHTTPServer(server_address, SimpleHandlerWithMutex)
    
    print('Starting server on port 5000...')
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
