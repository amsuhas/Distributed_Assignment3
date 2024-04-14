import http.client
import time
import asyncio
import random
from sortedcontainers import SortedDict
import docker
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
import threading
import json
import math
import threading
import copy
import os
import mysql.connector
import mysql.connector.pooling
import asyncio
from aiohttp import web

# Create a connection pool
connection = mysql.connector.pooling.MySQLConnectionPool(
    host="lb_database",
    user="root",
    password="password"
)

cursor = connection.cursor()




class Metadata:
    def __init__(self):
        cursor.execute("CREATE DATABASE IF NOT EXISTS Metadata")
        cursor.execute("USE Metadata")

    def add_shard(self, shard_id, shard_size,shard_id_low):
        cursor.execute("USE Metadata")
        # print(f"Inside add_shard function. adding {shard_id} info to ShardT")
        insert_query = f"INSERT INTO ShardT (Stud_id_low, Shard_id, Shard_size) VALUES ({shard_id_low}, {shard_id}, {shard_size});"
        cursor.execute(insert_query)
        connection.commit() 

    def remove_shard(self, shard_id):
        cursor.execute("USE Metadata")
        # print(f"Removing shard:{shard_id} from ShardT")
        delete_query = f"DELETE FROM ShardT WHERE Shard_id = {shard_id};"
        cursor.execute(delete_query)
        connection.commit()
    
    def add_server(self, server_id, shard_list):
        # connection = connection_pool.get_connection()
        # cursor = connection.cursor()
        cursor.execute("USE Metadata")
        # print(f"adding server:{server_id} having shard_list: {shard_list} in MapT")
        for shard in shard_list:
            insert_query = f"INSERT INTO MapT (Shard_id, Server_id, Primary) VALUES ({shard}, {server_id}, FALSE);"
            cursor.execute(insert_query)
        connection.commit()
        # cursor.close()
        # connection.close()

    def remove_server(self, server_id):
        # connection = connection_pool.get_connection()
        # cursor = connection.cursor()
        cursor.execute("USE Metadata")
        # print(f"Removing server:{server_id} from MapT")
        delete_query = f"DELETE FROM MapT WHERE Server_id = {server_id};"
        cursor.execute(delete_query)
        connection.commit()
        # cursor.close()
        # connection.close()

    def get_shards(self, server_id):
        # connection = connection_pool.get_connection()
        # cursor = connection.cursor()
        cursor.execute("USE Metadata")
        # print(f"Getting shards for server:{server_id}")
        select_query = f"SELECT Shard_id FROM MapT WHERE Server_id = {server_id};"
        cursor.execute(select_query)
        shard_list = cursor.fetchall()
        for i in range(len(shard_list)):
            shard_list[i] = shard_list[i][0]
        # cursor.close()
        # connection.close()
        return shard_list

    def get_all_shards(self):
        # connection = connection_pool.get_connection()
        # cursor = connection.cursor()
        cursor.execute("USE Metadata")
        # print(f"Getting all shards")
        select_query = f"SELECT Stud_id_low, Shard_id, Shard_size FROM ShardT;"
        cursor.execute(select_query)
        shard_list = cursor.fetchall()
        # cursor.close()
        # connection.close()
        return shard_list
    
    def get_shard_id(self, stud_id):
        cursor.execute("USE Metadata")
        # print(f"Getting shard_id for stud_id:{stud_id}")
        select_query = f"SELECT Shard_id FROM ShardT WHERE Stud_id_low <= {stud_id} AND Stud_id_low + Shard_size > {stud_id};"
        cursor.execute(select_query)
        shard_id = cursor.fetchall()
        if len(shard_id) == 0:
            return None
        return shard_id[0][0]
    
    def get_server_id(self, shard_id):
        cursor.execute("USE Metadata")
        # print(f"Getting server_id for shard_id:{shard_id}")
        select_query = f"SELECT Server_id FROM MapT WHERE Shard_id = {shard_id};"
        cursor.execute(select_query)
        server_ids = cursor.fetchall()
        return server_ids
    
    def get_primary_server(self, shard_id):
        # connection = connection_pool.get_connection()
        # cursor = connection.cursor()
        cursor.execute("USE Metadata")
        print(f"Getting primary server for shard_id:{shard_id}")
        select_query = f"SELECT Server_id FROM MapT WHERE Shard_id = {shard_id} AND Primary = TRUE;"
        cursor.execute(select_query)
        server_id = cursor.fetchone()[0]
        # cursor.close()
        # connection.close()
        return server_id
    
    def set_primary_server(self, shard_id, server_id):
        # connection = connection_pool.get_connection()
        # cursor = connection.cursor()
        cursor.execute("USE Metadata")
        print(f"Setting primary server for shard_id:{shard_id} to server_id:{server_id}")
        update_query = f"UPDATE MapT SET Primary = TRUE WHERE Shard_id = {shard_id} AND Server_id = {server_id};"
        cursor.execute(update_query)
        connection.commit()
        # cursor.close()
        # connection.close()
        
    def get_all_servers(self):
        cursor.execute("USE Metadata")
        # print(f"Getting all servers")
        select_query = f"SELECT DISTINCT Server_id FROM MapT;"
        cursor.execute(select_query)
        server_ids = cursor.fetchall()
        for i in range(len(server_ids)):
            server_ids[i] = server_ids[i][0]
        return server_ids
    
    def get_server_ids(self, shards):
        servers = set()
        for shard in shards:
            server_ids = Metadata.get_server_id(shard)
            for server_id in server_ids:
                servers.add(server_id)
        return servers
    
    def get_secondary_servers(self, shard, primary_server):
        cursor.execute("USE Metadata")
        select_query = f"SELECT Server_id FROM MapT WHERE Shard_id = {shard} AND Server_id != {primary_server};"
        cursor.execute(select_query)
        secondary_servers = cursor.fetchall()
        for i in range(len(secondary_servers)):
            secondary_servers[i] = secondary_servers[i][0]
        return secondary_servers
    
        




metadata_obj=Metadata()




def send_request(host='server', port=5000, path='/config',payload={},method='POST'):
    headers = {'Content-type': 'application/json'}

    json_payload = json.dumps(payload)
    
    connection = http.client.HTTPConnection(host, port)
    # print(f'Sending {method} request to {host}:{port}{path} with payload: {json_payload}')
    connection.request(method, path, json_payload, headers)

    response = connection.getresponse()
    # print(f'Status: {response.status}')

    # print('Response:')
    response_data = response.read().decode('utf-8')
    json_response = json.loads(response_data)
    connection.close()
    # print(json_response)
    return json_response, response.status











def send_del_request_rm(host='load_balancer', port=5000, path='/rm', server = ""):
    print("/rm")
    payload = {
        "n":1,
        "servers": [server]
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
    
def send_post_request_add(host='load_balancer', port=5000, path='/add', shards = [], server = ""):
        print("/add")
        payload = {
            "n":1,
            "new_shards": [],
            "servers": {
                server: shards
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

def send_get_request_with_timeout(host_name='localhost', port=5000, path='/'):
    global metadata_obj
    # global servers_obj
    try:
        connection = http.client.HTTPConnection(host_name, port, timeout=5)    
        # print(f"Sending heartbeat request to  {host_name}")
        connection.request('GET', path)
        response = connection.getresponse()
        response.read()
        connection.close()
    except Exception as e:
        # print(e)
        # print(f"ERROR!! Heartbeat response not received from {host_name}")
        shard_list = metadata_obj.get_shards(int(host_name[6:]))
        for i in shard_list:
            shard_list[i] = 'sh' + shard_list[i]
        send_del_request_rm('load_balancer', 5000, '/rm', host_name)
        send_post_request_add('load_balancer', 5000, '/add', shard_list, host_name)
    return

def thread_heartbeat():
    # global servers_obj
    # print("Heartbeat thread started")
    global metadata_obj
    while(1):
        # with servers_obj.mutex:
        host_list = metadata_obj.get_all_servers()
        # for server_id in host_list:
        #     host_list.append("server" + str(server_id))
        for host_name in host_list:
            host_name = "server" + str(host_name)
            send_get_request_with_timeout(host_name, 5000, '/heartbeat')
        time.sleep(120)











def server_log_count(shards, server_id):
    server_name = "server" + str(server_id)
    payload = {
        "shards": shards
    }
    response, status = send_request(server_name, 5000, '/log_count', payload, 'GET')
    return response, status

def server_set_primary(shard, server_id, secondary_servers):
    server_name = "server" + str(server_id)
    payload = {
        "shard": shard,
        "secondary": secondary_servers
    }
    response, status = send_request(server_name, 5000, '/set_primary', payload, 'PUT')
    return response, status

def primary_elect(content):
    print("Primary handler")
    # content = await request.json()

    shards = content['shards']
    best_primary_server = {}
    for shard in shards:
        best_primary_server[shard] = (-1, -1)
    server_ids = Metadata.get_server_ids(shards)
    
    for server_id in server_ids:
        try:
            response, status = server_log_count(shards, server_id)
        except:
            continue
        
        if(status != 200):
            continue
        response_log_count = response['log_count']
        for shard in response_log_count.keys():
            count = response_log_count[shard]
            if(count > best_primary_server[shard][1]):
                best_primary_server[shard] = (server_id, count)      
    
    for shard in shards:
        if(best_primary_server[shard][0] == -1):
            continue
            # return web.json_response({"message": "<Error> No server available for atleast a shard", "status": "failure"}, status = 400)
        
        set_primary_query = f"UPDATE MapT SET Primary = TRUE WHERE Shard_id = {shard} AND Server_id = {best_primary_server[shard][0]};"
        cursor.execute(set_primary_query)
        connection.commit()
        
        secondary_servers = metadata_obj.get_secondary_servers(shard, best_primary_server[shard][0])
        try:
            response, status = server_set_primary(shard, best_primary_server[shard][0], secondary_servers)
        except:
            continue

    return 1
    




class Post_Handler:
    async def primary_handler(self, request):
       
        if primary_elect(request):
            return web.json_response({"message": "Primary server set successfully", "status": "success"}, status = 200)

    
    async def add_server_handler(self, request):
        print("Add server handler")
        content = await request.json()
        shards = content['shards']
        server_id = content['server_id']
        metadata_obj.add_server(server_id, shards)
        return web.json_response({"message": "Server added successfully", "status": "success"}, status = 200)


class Delete_Handler:
    async def remove_server_handler(self, request):
        print("Delete server handler")
        content = await request.json()
        server_id = content['server_id']
        shard_list = metadata_obj.get_shards(server_id)
        for shard in shard_list:
            metadata_obj.remove_server(server_id)
        return web.json_response({"message": "Server deleted successfully", "status": "success"}, status = 200)
    

handle_post = Post_Handler()
handle_delete = Delete_Handler()

app = web.Application()
app.router.add_post('/primary_elect', handle_post.primary_handler)
app.router.add_post('/add_server', handle_post.add_server_handler)
app.router.add_post('/init', handle_post.init_handler)
app.router.add_delete('/remove_server', handle_delete.remove_server_handler)

if __name__ == "__main__":
    print("Starting shard manager")
    try:
        web.run_app(app, port=5000)
        heart_beat_thread = threading.Thread(target=thread_heartbeat)
        heart_beat_thread.start()
        heart_beat_thread.join()
    except:
        print("Error in starting shard manager")
        exit(0)
        
        