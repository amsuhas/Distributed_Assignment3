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
connection = mysql.connector.connect(
    host="lb_database",  # Container name of MySQL
    user="root",
    password="password"
)

cursor = connection.cursor()




class Metadata:
    def __init__(self):
        # cursor.execute("CREATE DATABASE IF NOT EXISTS Metadata")
        cursor.execute("USE Metadata")
        # cursor.execute("SET SESSION query_cache_type = OFF")

    def add_shard(self, shard_id, shard_size,shard_id_low):
        # cursor.execute("USE Metadata")
        # print(f"Inside add_shard function. adding {shard_id} info to ShardT")
        insert_query = f"INSERT INTO ShardT (Stud_id_low, Shard_id, Shard_size) VALUES ({shard_id_low}, {shard_id}, {shard_size});"
        cursor.execute(insert_query)
        connection.commit() 

    def remove_shard(self, shard_id):
        # cursor.execute("USE Metadata")
        # print(f"Removing shard:{shard_id} from ShardT")
        delete_query = f"DELETE FROM ShardT WHERE Shard_id = {shard_id};"
        cursor.execute(delete_query)
        connection.commit()
    
    def add_server(self, server_id, shard_list):
        # connection = connection_pool.get_connection()
        # cursor = connection.cursor()
        # cursor.execute("USE Metadata")
        # print(f"adding server:{server_id} having shard_list: {shard_list} in MapT")
        for shard in shard_list:
            insert_query = f"INSERT INTO MapT (Shard_id, Server_id, Is_Primary) VALUES ({shard}, {server_id}, 0);"
            cursor.execute(insert_query)
        connection.commit()
        # cursor.close()
        # connection.close()

    def remove_server(self, server_id):
        # connection = connection_pool.get_connection()
        # cursor = connection.cursor()
        # cursor.execute("USE Metadata")
        # print(f"Removing server:{server_id} from MapT")
        delete_query = f"DELETE FROM MapT WHERE Server_id = {server_id};"
        cursor.execute(delete_query)
        connection.commit()
        # cursor.close()
        # connection.close()

    def get_shards(self, server_id):
        # connection = connection_pool.get_connection()
        # cursor = connection.cursor()
        # cursor.execute("USE Metadata")
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
        # cursor.execute("USE Metadata")
        # print(f"Getting all shards")
        select_query = f"SELECT Stud_id_low, Shard_id, Shard_size FROM ShardT;"
        cursor.execute(select_query)
        shard_list = cursor.fetchall()
        # cursor.close()
        # connection.close()
        return shard_list
    
    def get_shard_id(self, stud_id):
        # cursor.execute("USE Metadata")
        # print(f"Getting shard_id for stud_id:{stud_id}")
        select_query = f"SELECT Shard_id FROM ShardT WHERE Stud_id_low <= {stud_id} AND Stud_id_low + Shard_size > {stud_id};"
        cursor.execute(select_query)
        shard_id = cursor.fetchall()
        if len(shard_id) == 0:
            return None
        return shard_id[0][0]
    
    def get_server_id(self, shard_id):
        # cursor.execute("USE Metadata")
        # print(f"Getting server_id for shard_id:{shard_id}")
        select_query = f"SELECT Server_id FROM MapT WHERE Shard_id = {shard_id};"
        cursor.execute(select_query)
        server_ids = cursor.fetchall()
        return server_ids
    
    def get_primary_server(self, shard_id):
        # connection = connection_pool.get_connection()
        # cursor = connection.cursor()
        # cursor.execute("USE Metadata")
        print(f"Getting primary server for shard_id:{shard_id}")
        select_query = f"SELECT Server_id FROM MapT WHERE Shard_id = {shard_id} AND Is_Primary = 1;"
        cursor.execute(select_query)
        server_id = cursor.fetchone()[0]
        # cursor.close()
        # connection.close()
        return server_id
    
    def set_primary_server(self, shard_id, server_id):
        # connection = connection_pool.get_connection()
        # cursor = connection.cursor()
        # cursor.execute("USE Metadata")
        print(f"Setting primary server for shard_id:{shard_id} to server_id:{server_id}")
        update_query = f"UPDATE MapT SET Is_Primary = 1 WHERE Shard_id = {shard_id} AND Server_id = {server_id};"
        cursor.execute(update_query)
        connection.commit()
        # cursor.close()
        # connection.close()
        
    def get_all_servers(self):
        # cursor.execute("USE Metadata")
        # print(f"Getting all servers")
        select_query = f"SELECT DISTINCT Server_id FROM MapT;"
        cursor.execute(select_query)
        server_ids = cursor.fetchall()
        # connection.commit()
        for i in range(len(server_ids)):
            print(server_ids[i])
            server_ids[i] = server_ids[i][0]
        return server_ids
    
    def get_server_ids(self, shards):
        servers = []
        for shard in shards:
            shard_id = int(shard[2:])
            server_ids = metadata_obj.get_server_id(shard_id)
            for server_id in server_ids:
                if(server_id[0] not in servers):
                    servers.append(server_id[0])
        return servers
    
    def get_secondary_servers(self, shard_id, primary_server_id):
        # cursor.execute("USE Metadata")
        select_query = f"SELECT Server_id FROM MapT WHERE Shard_id = {shard_id} AND Server_id != {primary_server_id};"
        cursor.execute(select_query)
        secondary_servers = cursor.fetchall()
        for i in range(len(secondary_servers)):
            secondary_servers[i] = secondary_servers[i][0]
        return secondary_servers
    
    def get_primary_shards(self, server_id):
        # cursor.execute("USE Metadata")
        select_query = f"SELECT Shard_id FROM MapT WHERE Server_id = {server_id} AND Is_Primary = 1;"
        cursor.execute(select_query)
        primary_shards = cursor.fetchall()
        for i in range(len(primary_shards)):
            primary_shards[i] = primary_shards[i][0]
        return primary_shards
    
        




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
    # server[0] = 'S'
    server = 'Server' + str(server[6:])

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
        server = 'Server' + str(server[6:])
        payload = {
            "n":1,
            "new_shards": [],
            "servers": {
                server: shards
            }
        }
        print(payload)
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
        print(f"Sending heartbeat request to  {host_name}")
        connection.request('GET', path)
        response = connection.getresponse()
        response.read()
        connection.close()
    except Exception as e:
        print(e)
        print(f"ERROR!! Heartbeat response not received from {host_name}")
        shard_list = metadata_obj.get_shards(int(host_name[6:]))
        print(shard_list)
        for i in range(len(shard_list)):
            shard_list[i] = 'sh' + str(shard_list[i])
        send_del_request_rm('load_balancer', 5000, '/rm', host_name)
        send_post_request_add('load_balancer', 5000, '/add', shard_list, host_name)
    return

def thread_heartbeat():
    # global servers_obj
    time.sleep(60)
    print("Heartbeat thread started")
    global metadata_obj
    while(1):
        # with servers_obj.mutex:
        connection.commit()
        host_list = metadata_obj.get_all_servers()
        # for server_id in host_list:
        #     host_list.append("server" + str(server_id))
        print("host_list", host_list)
        for host_name in host_list:
            host_name = "server" + str(host_name)
            print("host_name heartbeat", host_name)
            send_get_request_with_timeout(host_name, 5000, '/heartbeat')
        time.sleep(240)











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
        "secondary_servers": secondary_servers
    }
    print("sending ste primary payload", payload)
    print(server_name)
    response, status = send_request(server_name, 5000, '/set_primary', payload, 'PUT')
    return response, status

def primary_elect(shards):
    print("Is_Primary handler")
    

    # shards = content['shards']
    best_primary_server = {}
    for shard in shards:
        best_primary_server[shard] = (-1, -1)
        
    server_ids = metadata_obj.get_server_ids(shards)
    
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
        shard_id = int(shard[2:])
        set_primary_query = f"UPDATE MapT SET Is_Primary = 1 WHERE Shard_id = {shard_id} AND Server_id = {best_primary_server[shard][0]};"
        cursor.execute(set_primary_query)
        connection.commit()
        
        secondary_servers = metadata_obj.get_secondary_servers(shard_id, best_primary_server[shard][0])
        try:
            response, status = server_set_primary(shard, best_primary_server[shard][0], secondary_servers)
            print(f"Primary set for shard:{shard} to server:{best_primary_server[shard][0]}")
        except:
            continue

    return 1
    




class Post_Handler:
    
    async def add_server_handler(self, request):
        print("Add server handler")
        content = await request.json()
        print(content, " in add server handler")
        check = primary_elect(content['new_shards'])
        # servers_dict = content['servers']
        # shard_ids = set()
        # for key in servers_dict.keys():
        #     for shard_id in servers_dict[key]:
        #         shard_ids.add(shard_id)
                
        # for shard_id in shard_ids:
        #     primary_server_id = metadata_obj.get_primary_server(shard_id)
        #     secondary_servers = metadata_obj.get_secondary_servers(shard_id, primary_server_id)
        #     try:
        #         response, status = server_set_primary("sh" + str(shard_id), primary_server_id, secondary_servers)
        #         print(f"Primary set for shard:sh{shard_id} to server:{primary_server_id} lesgo")
        #     except:
        #         print("Error in setting primary")
        #         continue
            
        
        
        return web.json_response({"message": "Server added successfully", "status": "success"}, status = 200)


class Delete_Handler:
    async def remove_server_handler(self, request):
        print("Delete server handler")
        content = await request.json()
        server_ids = content['server_ids']
        #elect primary for
        for server_id in server_ids:
            primary_shards=metadata_obj.get_primary_shards(server_id)
            #call primary elect
            new_p_list = []
            if len(primary_shards) > 0:
                for i in primary_shards:
                    new_p_list.append('sh' + str(i))
                primary_elect(new_p_list)
            # if len(primary_shards) > 0:
            #     primary_elect(primary_shards)

        # send the updated list to the primary of other shards
        shard_list = []
        for server_id in server_ids:
            shard_list_t= metadata_obj.get_shards(server_id)
            for i in shard_list_t:
                if i not in shard_list:
                    shard_list.append(i)

        for shard in shard_list:
            primary_server = metadata_obj.get_primary_server(shard)
            secondary_servers = metadata_obj.get_secondary_servers(shard, primary_server)
            #delete server from secondary servers
            # secondary_servers = [x for x in secondary_servers if x != server_id]

            try:
                response, status = server_set_primary(shard, primary_server, secondary_servers)
            except:
                continue            
                
        
        return web.json_response({"message": "Server deleted successfully", "status": "success"}, status = 200)
    

handle_post = Post_Handler()
handle_delete = Delete_Handler()

app = web.Application()
app.router.add_post('/add_server', handle_post.add_server_handler)
# app.router.add_post('/init', handle_post.init_handler)
app.router.add_delete('/remove_server', handle_delete.remove_server_handler)

if __name__ == "__main__":
    try:
        heart_beat_thread = threading.Thread(target=thread_heartbeat)
        heart_beat_thread.start()
        print("Starting shard manager")
        web.run_app(app, port=5000)
        heart_beat_thread.join()
    except:
        print("Error in starting shard manager")
        exit(0)
        
        