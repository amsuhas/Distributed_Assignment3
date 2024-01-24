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
import threading
import copy
import os

num_retries = 3

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



def send_get_request_with_timeout(host_name='localhost', port=5000, path='/'):
    try:
        connection = http.client.HTTPConnection(host_name, port, timeout=5)    
        print("Sending heartbeat request to " + host_name)
        connection.request('GET', path)
        response = connection.getresponse()
        response.read()
        connection.close()
    except Exception as e:
        with shared_data.mutex:
            shared_data.rm_server(host_name)
            shared_data.add_server()
        print(e)
        print("ERROR!! Heartbeat response not received from " + host_name)

def thread_heartbeat():
    while(1):
        with shared_data.mutex:
            host_list = []
            for keys in shared_data.serv_dict:
                host_list.append(keys)
            # host_list = copy.deepcopy(shared_data.serv_dict)
        for host_name in host_list:
                send_get_request_with_timeout(host_name, 5000, '/heartbeat')
        time.sleep(5)

    
    


serv_list = []





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
            prime_multiplier = 37
            magic_number = 0x5F3759DF
            constant_addition = 11

            nindex = ((self.counter*self.counter + j*j + 2 * j + 25) * prime_multiplier) ^ magic_number
            nindex = (nindex + constant_addition) % self.buf_size
            nindex = (nindex ^ (nindex & (nindex ^ (nindex - 1)))) % self.buf_size  # Bitwise AND operation
            nindex+=self.buf_size
            nindex%=self.buf_size
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
        # print(li)
        return li

    def client_hash(self,r_id):
        prime_multiplier = 31
        magic_number = 0x5F3759DF
        constant_addition = 7

        nindex = ((r_id + 2 * r_id + 17) * prime_multiplier) ^ magic_number
        nindex = (nindex + constant_addition) % self.buf_size
        nindex = (nindex ^ (nindex & (nindex ^ (nindex - 1)))) % self.buf_size  # Bitwise AND operation
        nindex+=self.buf_size
        nindex%=self.buf_size
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
        # print(self.serv_id_dict)
        if(jp!=512 and len(self.serv_id_dict) != 0):
            lower_bound_key = self.serv_id_dict.bisect_left(nindex)
            if lower_bound_key == len(self.serv_id_dict):
                return self.cont_hash[self.serv_id_dict.iloc[0]][0], ((nindex-1)+self.buf_size)%self.buf_size
            else:
                return self.cont_hash[self.serv_id_dict.iloc[lower_bound_key]][0], ((nindex-1)+self.buf_size)%self.buf_size
        else:
            return None
        

    def rm_server(self,host_name):
        indexes=self.serv_dict[host_name][0]
        for ind in indexes:
            self.cont_hash[ind][0]=None
            del self.serv_id_dict[ind]
        self.num_serv-=1
        container = self.serv_dict[host_name][1]
        del self.serv_dict[host_name]
        time.sleep(5)
        try:
            container.stop()
            container.remove()
        except:
            print("No such container found!!")

    def add_server(self):
        self.num_serv += 1
        self.counter += 1
        while(1):
            new_name = "server" + str(self.counter)
            if new_name in self.serv_dict:
                self.counter += 1
            else:
                break
        if self.buf_size - self.num_vservs*self.num_serv <self.num_vservs:
            self.counter-=1
            self.num_serv-=1
            print("ERROR!! Buffer size exceeded")
            return
        serv_listid = self.get_hash(new_name)
        environment_vars = {'ID': self.counter}
        container = client.containers.run("server_image", detach=True, hostname = new_name, name = new_name, network ="my_network", environment=environment_vars)
        self.serv_dict[new_name] = [serv_listid,container,0]
        time.sleep(1)
        


        




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
                for i in range(len(name_servs)):
                    if(name_servs[i] in shared_data.serv_dict):
                        self.send_response(400)
                        self.send_header('Content-type', 'application/json')
                        self.end_headers()
                        server_response = {"message": "<Error> Same name server already exists", "status": "failure"}
                        response_str = json.dumps(server_response)
                        self.wfile.write(response_str.encode('utf-8'))
                        return
                for i in range(num_servs):
                    shared_data.num_serv += 1
                    shared_data.counter += 1
                    if(i>=len(name_servs)):
                        while(1):
                            new_name = "server" + str(shared_data.counter)
                            if new_name in shared_data.serv_dict:
                                shared_data.counter += 1
                            else:
                                break
                    else:
                        new_name = name_servs[i]

                    
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
                    environment_vars = {'ID': shared_data.counter}
                    container = client.containers.run("server_image", detach=True, hostname = new_name, name = new_name, network ="my_network", environment=environment_vars)
                    shared_data.serv_dict[new_name] = [serv_listid,container,0]
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
        elif(self.path == '/info'):
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            counter_dict = {key: value[2] for key, value in shared_data.serv_dict.items()}
            # server_response = {"message": {"N": shared_data.num_serv, "replicas": [hostname for hostname in shared_data.serv_dict.keys()]}, "status": "successful"}
            response_str = json.dumps(counter_dict)
            self.wfile.write(response_str.encode('utf-8'))
            return

        else:
            rid = random.randrange(99999, 1000000, 1)
            cnt = num_retries
            while(1):
                cnt -= 1
                with shared_data.mutex:
                    serv_id, index = shared_data.client_hash(rid)
                if(cnt == 0):
                    self.send_response(500)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    server_response = {"message": "<Error> Server not found", "status": "failure"}
                    response_str = json.dumps(server_response)
                    self.wfile.write(response_str.encode('utf-8'))
                    with shared_data.mutex:
                        shared_data.cont_hash[index][1] = None
                    return
                if serv_id == None:
                    with shared_data.mutex:
                        shared_data.cont_hash[index][1] = None
                    continue
                else:
                    # port = ports[serv_id]
                    shared_data.serv_dict[serv_id][2] += 1
                    response = send_get_request(serv_id, 5000, self.path)
                    # response = send_get_request('localhost', port, self.path)
                    self.send_response(response.status)
                    self.send_header('Content-type', response.getheader('Content-type'))
                    self.end_headers()
                    self.wfile.write(response.read())
                    with shared_data.mutex:
                        shared_data.cont_hash[index][1] = None
                    return


if __name__ == '__main__':
    server_address = ('', 5000)
    httpd = ThreadingHTTPServer(server_address, SimpleHandlerWithMutex)
    n = int(os.environ.get('NUM_INIT_SERVER'))
    with shared_data.mutex:
        for _ in range(n):
            shared_data.add_server()
        
    print('Starting server on port 5000...')
    try:
        heart_beat_thread = threading.Thread(target=thread_heartbeat)
        heart_beat_thread.start()
        httpd.serve_forever()
        heart_beat_thread.join()

    except KeyboardInterrupt:
        print('Server is shutting down...')
        httpd.shutdown()
        exit()
    
    

# async