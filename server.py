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

my_server_id = os.environ.get('ID')
connection = {}
cursor = {}
connection = mysql.connector.connect(
    host="localhost",
    user="myuser",
    password="mypass",
    database="Student_info"
)
cursor = connection.cursor()

# Create a connection pool
# connection_metadata = mysql.connector.pooling.MySQLConnectionPool(
#     host="lb_database",
#     user="root",
#     password="password"
# )

# cursor = connection.cursor()
# update_idx_dict = {}
# primary_shards = []
# log_shards = {}
log_count_shards = {}
log_file_shards = {}
is_primary_dict = {}
# last_query_shards = {}


def send_request(host='server', port=5000, path='/config',payload={},method='POST'):
    headers = {'Content-type': 'application/json'}

    json_payload = json.dumps(payload)
    
    connection = http.client.HTTPConnection(host, port)
    print(f'Sending {method} request to {host}:{port}{path} with payload: {json_payload}')
    connection.request(method, path, json_payload, headers)

    response = connection.getresponse()
    print(f'Status: {response.status}')

    print('Response:')
    response_data = response.read().decode('utf-8')
    json_response = json.loads(response_data)
    connection.close()
    print(json_response)
    return json_response, response.status     
        
def manage_request_non_primary(shard, payload, endpoint, method):
    decision = "commit" 
    if is_primary_dict[shard][0] == 1:
        # log_shards[shard] = f"writing data in {shard}: \n{studs_data}"
        server_ids = is_primary_dict[shard][1]
        total_servers = len(server_ids)
        success_count = 1
        for server_id in server_ids:
            print(server_id, my_server_id)
            # if(server_id == "load_balancer"):
            #     continue
            if(int(server_id) == int(my_server_id)):
                continue
            server_name = "server" + str(server_id)
            try:
                response, status_code = send_request(server_name, 5000, endpoint, payload, method)
                print("try me chale gaya")
                if response['status'] == "success":
                    success_count += 1
            except:
                print(f"Couldnot connect to server {server_id}")
                continue
            
                
        
        if(success_count > total_servers/2):
            # commit
            print("Commiting write")
            # cursor[shard].commit()
        else:
            # rollback
            print("Rolling back write")
            # cursor[shard].rollback()
            decision = "rollback"
            
            
        decision, response = decision_func({"shard": shard, "decision": decision})
        for server_id in server_ids:
            # if(server_id == "load_balancer"):
            #     print("load balancer continuing")
            #     continue
            if(server_id == my_server_id):
                continue
            try:
                response, status = server_decision(shard, decision, server_id)
                print(response, status)
            except:
                print(f"Couldnot connect to server {server_id} for decision")
                continue
    
    return decision
        
def server_decision(shard, decision, server_id):
    payload = {
        "shard": shard,
        "decision": decision
    }
    server_name = "server" + str(server_id)
    response, status = send_request(server_name, 5000, '/decision', payload, 'POST')
    return response, status
    
    
    
    
    
def decision_func(payload):
    shard = payload.get('shard')
    decision = payload.get('decision')
    
    # read last line from the log file which is a json object
    logs_file = log_file_shards[shard]
    # logs_file.seek(0, 2)
    # logs_file.seek(logs_file.tell() - 1, 0)
    # while logs_file.read(1) != b'\n':
    #     logs_file.seek(logs_file.tell() - 2, 0)
    # last_line = logs_file.readline().decode()
    # last_line = json.loads(last_line)


    logs_file.seek(0, 2)
    # Start reading from the end until a newline character is found
    pos = logs_file.tell()
    while pos > 0:
        pos -= 1
        logs_file.seek(pos)
        if logs_file.read(1) == '\n':
            break
    # Read the last line
    last_line = logs_file.readline().strip()
    print(type(last_line))
    print(last_line)
    # Parse the last line as a JSON object
    try:
        last_line = json.loads(last_line)
        # return json_object
    except json.JSONDecodeError as e:
        print("Error decoding JSON:", e)
        # return None
    
    if decision == "commit":
        # connection.commit()
        # last_query_shards[shard].execute_query()
        
        
        
        query = Query(last_line["type"], last_line["data"])
        query.execute_query()
        
        response_json = {
            "message": f'{shard} {last_line["type"]} query committed',
            "status": "success"
        }
        print("Committing query responding")
        
        return decision, response_json
        # return web.json_response(response_json, status=200)
    else:
        # connection.rollback()
        response_json = {
            "message": f'{shard} {last_line["type"]} query rolled back/aborted',
            "status": "success"
        }
        print("Rolling back query responding")
        return decision, response_json
        # return web.json_response(response_json, status=200)
    
    
        
        
        
class Get_handler:
    async def heartbeat_handler(self, request):
        print("Received heartbeat request\n")
        return web.json_response({}, status=200)
    
    async def copy_handler(self, request):
        print("Received copy request\n")
        
        payload = await request.json()
        
        shards = payload.get('shards')


        response_data = {}



        for shard in shards:
            # use_query = f"USE {shard};"
            # cursor.execute(use_query)
            query = f"SELECT * FROM {shard};"
            cursor.execute(query)
            
            # note 
            rows = cursor.fetchall()
            res = ""

            out_list = []
            res = {}
            for row in rows:
                res["Stud_id"] = str(row[0])
                res["Stud_name"] = row[1]
                res["Stud_marks"] = row[2]

                out_list.append(copy.deepcopy(res))
                res = {}

            response_data[shard] = out_list

        response_data["status"] = "success"
        print(response_data)
        return web.json_response(response_data, status=200)

    async def log_count_handler(self, request):
        print("Received log count request\n")
        payload = await request.json()
        
        shards = payload.get('shards')
        
        response_data = {}
        data = {}
        for shard in shards:
            if(shard not in log_count_shards.keys()):
                continue
            data[shard] = log_count_shards[shard]
            
        response_data["log_count"] = data
        response_data["status"] = "success"
        print(response_data)
        return web.json_response(response_data, status=200)
    
    async def read_all_handler(self, request):
        print("Received read all request\n")
        payload = await request.json()
        response_data = {}
        data = {}
        shards = []
        for shard in log_count_shards.keys():
            shards.append(shard)
        for shard in shards:
            # use_query = f"USE {shard};"
            # cursor.execute(use_query)
            query = f"SELECT * FROM {shard};"
            cursor.execute(query)
            
            # note 
            rows = cursor.fetchall()

            out_list = []
            res = {}
            for row in rows:
                res["Stud_id"] = str(row[0])
                res["Stud_name"] = row[1]
                res["Stud_marks"] = row[2]

                out_list.append(copy.deepcopy(res))
                res = {}

            response_data[shard] = out_list
            
        # response_data["data"] = data
        response_data["status"] = "success"
        print(response_data)
        return web.json_response(response_data, status=200)
        


    
class Post_handler:
    async def config_handler(self, request):
        print("Received config request\n")
        # return web.json_response({}, status=200)
        global connection
        global cursor
        global my_server_id
        payload=await request.json()

        schema = payload.get('schema')
        shards = payload.get('shards')

        tables_created = ""
        
        dict={}
        dict['Number'] = 'INT'
        dict['String'] = 'VARCHAR(255)'
        
        # create_database_query = f"CREATE DATABASE IF NOT EXISTS Student_info;"
        # cursor.execute(create_database_query)
        # connection.commit()
        # cursor.execute("USE Student_info;")
        
        for shard in shards:
            
            # connection[shard] = mysql.connector.connect(
            #     host="localhost",
            #     user="myuser",
            #     password="mypass"
            #     # database="Student_info"
            # )
            # cursor[shard] = connection[shard].cursor()
            
            log_count_shards[shard] = 0
            is_primary_dict[shard] = (0, [])
            # I want to have both read and write access to the log file
            log_file_shards[shard] = open(f"{shard}_log.txt", "a+")
            
            # update_idx_dict[shard] = 0
            # table_name = shard
            # columns = ', '.join([f"{col} {dict[dtype]}" for col, dtype in zip(schema['columns'], schema['dtypes'])])
            # # print(columns)
            # create_table_query = f"CREATE TABLE {table_name} ({columns});"
            
            # database_name = shard
            # create_database_query = f"CREATE DATABASE IF NOT EXISTS {database_name};"
            
            table_name = shard
            columns = ', '.join([f"{col} {dict[dtype]}" for col, dtype in zip(schema['columns'], schema['dtypes'])])
            # print(columns)
            create_table_query = f"CREATE TABLE {table_name} ({columns});"
            

            # print(create_database_query)
            # print(create_table_query)
            # cursor[shard].execute(create_database_query)
            cursor.execute(create_table_query)
            connection.commit()
            tables_created += (f"server{my_server_id}:{table_name}, ")
        tables_created =  tables_created[:-2]
        tables_created += (" configured")

        response_data = {'message': tables_created, 'status': 'success'}
        return web.json_response(response_data, status=200)
    
    async def read_handler(self, request):
        
        payload = await request.json()

        shard = payload.get('shard')
        stud_id_range = payload.get('Stud_id')


        low = int(stud_id_range.get('low'))
        high = int(stud_id_range.get('high'))


        response_data = {}

        # use_query = f"USE {shard};"
        # cursor.execute(use_query)

        query = f"SELECT * FROM {shard} WHERE Stud_id BETWEEN {low} AND {high};"
        cursor.execute(query)
        rows = cursor.fetchall()


        out_list = []
        res = {}
        for row in rows:
            res["Stud_id"] = str(row[0])
            res["Stud_name"] = row[1]
            res["Stud_marks"] = row[2]

            out_list.append(copy.deepcopy(res))
            res = {}

        response_data["data"] = out_list

        response_data["status"] = "success"
        return web.json_response(response_data, status=200)
    
    async def write_handler(self, request):        
        print("Received write request\n")
        payload = await request.json()

        shard = payload.get('shard')
        # curr_idx = int(payload.get('curr_idx'))
        studs_data = payload.get('data')
        
        
        for stud in studs_data:
            print(stud)
            # sid = int(stud.get('Stud_id'))
            # sname = '\"' + stud.get('Stud_name') + '\"'
            # smarks = '\"' + stud.get('Stud_marks') + '\"'
            # check_query = f"SELECT COUNT(*) FROM {shard} WHERE Stud_id = {sid};"
            # cursor.execute(check_query)
            # check = cursor.fetchone()[0]
            
            # if(check > 0):
            #     update_query = f"UPDATE {shard} SET Stud_id = {sid}, Stud_name = {sname}, Stud_marks = {smarks} WHERE Stud_id = {sid};"
            #     cursor.execute(update_query)    
            # else:
            #     write_query = f"INSERT INTO {shard}(Stud_id, Stud_name, Stud_marks) VALUES ({sid}, {sname}, {smarks});"
            #     cursor.execute(write_query)
            #     # curr_idx = curr_idx + 1
            
            # last_query_shards[shard] = Query("write", payload)
        
        log_entry = {}
        log_count_shards[shard] += 1
        log_entry['counter'] = log_count_shards[shard]
        log_entry['type'] = 'write'
        log_entry['data'] = payload
        log_file_shards[shard].write("\n")
        log_file_shards[shard].write(json.dumps(log_entry))
        # print new line to file
        # log_shards[shard] = f"writing data in {shard}: \n{studs_data}"
        if is_primary_dict[shard][0] == 1:
            decision = manage_request_non_primary(shard, payload, '/write', 'POST')
            
            if(decision == 'commit'):
                # connection.commit()
                response_json = {
                    "message": 'Data entries added',
                    # "current_idx": curr_idx,
                    "status": "success"
                }
                #note add the log in the file
                return web.json_response(response_json, status=200)
            else:
                response_json = {
                    "message": 'Write failed',
                    "status": "failure"
                }
                return web.json_response(response_json, status=404)
        else:
            response_json = {
                "message": 'Acknowledgement for addition of Data entries',
                # "current_idx": curr_idx,
                "status": "success"
            }
            #note add the log in the file
            return web.json_response(response_json, status=200)
        
        
        
        
        
    # async def updateid_handler(self, request):
    #     print("Received update index request\n")
    #     payload = await request.json()  
    #     shard = payload.get('shard')
    #     update_idx_dict[shard] = int(payload.get('update_idx'))
    #     response_json = {
    #         "message": 'Update index updated',
    #         "status": "success"
    #     }
    #     return web.json_response(response_json, status=200)  
    
    
    
    async def decision_handler(self, request):
        print("Received decision request\n")
        payload = await request.json()
        
        decision, response_json = decision_func(payload)
        
        # shard = payload.get('shard')
        # decision = payload.get('decision')
        
        # # read last line from the log file which is a json object
        # logs_file = log_file_shards[shard]
        # # logs_file.seek(0, 2)
        # # logs_file.seek(logs_file.tell() - 1, 0)
        # # while logs_file.read(1) != b'\n':
        # #     logs_file.seek(logs_file.tell() - 2, 0)
        # # last_line = logs_file.readline().decode()
        # # last_line = json.loads(last_line)


        # logs_file.seek(0, 2)
        # # Start reading from the end until a newline character is found
        # pos = logs_file.tell()
        # while pos > 0:
        #     pos -= 1
        #     logs_file.seek(pos)
        #     if logs_file.read(1) == '\n':
        #         break
        # # Read the last line
        # last_line = logs_file.readline().strip()
        # print(type(last_line))
        # print(last_line)
        # # Parse the last line as a JSON object
        # try:
        #     last_line = json.loads(last_line)
        #     # return json_object
        # except json.JSONDecodeError as e:
        #     print("Error decoding JSON:", e)
        #     # return None
        
        if decision == "commit":
            # connection.commit()
            # last_query_shards[shard].execute_query()
            
            
            
            # query = Query(last_line["type"], last_line["data"])
            # query.execute_query()
            
            # response_json = {
            #     "message": f'{shard} {last_line["type"]} query committed',
            #     "status": "success"
            # }
            # print("Committing query responding")
            
            return web.json_response(response_json, status=200)
        else:
            # connection.rollback()
            # response_json = {
            #     "message": f'{shard} {last_line["type"]} query rolled back/aborted',
            #     "status": "success"
            # }
            # print("Rolling back query responding")
            return web.json_response(response_json, status=200)
    


class Put_handler:
    async def update_handler(self, request):
        print("Received update request\n")
        payload = await request.json()
        
        shard = payload.get('shard')
        stud_id = int(payload.get('Stud_id'))
        data = payload.get('data')
        sid = int(data.get('Stud_id'))
        sname ='\"'+ data.get('Stud_name')+'\"'
        smarks = '\"'+data.get('Stud_marks')+'\"'
            
        # use_query = f"USE {shard};"
        # cursor[shard].execute(use_query)
        
        check_query = f"SELECT COUNT(*) FROM {shard} WHERE Stud_id = {sid};"
        cursor.execute(check_query)
        check = cursor.fetchone()[0]
        
        if check <= 0:
            return web.json_response({"error": f"Entry with Stud_id:{sid} does not exist in the given shard"}, status=400)
        
        # update_query = f"UPDATE {shard} SET Stud_id = {sid}, Stud_name = {sname}, Stud_marks = {smarks} WHERE Stud_id = {stud_id};"
        # cursor.execute(update_query)
        
        # last_query_shards[shard] = Query("update", payload)
        
        # connection[shard].commit()
        # response_json = {
        #     "message": f'Data entry for Stud_id:{stud_id} updated',
        #     "status": "success"
        # }
        # return web.json_response(response_json, status=200)
        
        log_entry = {}
        log_file_shards[shard].write("\n")
        log_count_shards[shard] += 1
        log_entry['counter'] = log_count_shards[shard]
        log_entry['type'] = 'update'
        log_entry['data'] = payload
        log_file_shards[shard].write(json.dumps(log_entry))
        # log_shards[shard] = f"Updating data in {shard} for Stud_id: {stud_id} with data:\n{data}"
            
        
            
        
        if(is_primary_dict[shard][0] == 1):
            decision = manage_request_non_primary(shard, payload, '/update', 'PUT')
            if(decision == 'commit'):
                # connection.commit()
                response_json = {
                    "message": f'Data entry for Stud_id:{stud_id} updated',
                    "status": "success"
                }
                return web.json_response(response_json, status=200)
            else:
                response_json = {
                    "message": 'Update failed',
                    "status": "failure"
                }
                return web.json_response(response_json, status=404)
        else:
            response_json = {
                "message": f'Acknowledgement for updation of dat entry for Stud_id:{stud_id}',
                "status": "success"
            }
            return web.json_response(response_json, status=200)    
        
        
    
    async def set_primary(self, request):
        print("Received set primary request\n")
        payload = await request.json()
        
        shard = payload.get('shard')
        secondary_servers = payload.get('secondary_servers')
        # for shard in shards:
            # primary_shards.append(shard)
        print(f"Primary shards set for {shard} with secondary servers: {secondary_servers}")
        is_primary_dict[shard] = (1, secondary_servers)
        response_json = {
            "message": 'Primary shards set',
            "status": "success"
        }
        
        return web.json_response(response_json, status=200)
            
        
        
        

class Delete_handler:
    async def del_handler(self, request):
        print("Received delete request\n")
        payload = await request.json()
        shard = payload.get('shard')
        sid = int(payload.get('Stud_id'))
        
        # use_query = f"USE {shard};"
        # cursor.execute(use_query)
        
        check_query = f"SELECT COUNT(*) FROM {shard} WHERE Stud_id = {sid};"
        cursor.execute(check_query)
        check = cursor.fetchone()[0]
        
        if check is None:
            return web.json_response({"error": f"Entry with Stud_id:{sid} does not exist in the given shard"}, status=400)
        
        if check <= 0:
            return web.json_response({"error": f"Entry with Stud_id:{sid} does not exist in the given shard"}, status=400)
        
        # delete_query = f"DELETE FROM {shard} WHERE Stud_id = {sid};"
        # cursor.execute(delete_query)
        
        # last_query_shards[shard] = Query("delete", payload)
        
        # connection[shard].commit()
        # update_idx_dict[shard] = update_idx_dict[shard] - 1
        log_entry = {}
        log_file_shards[shard].write("\n")
        log_count_shards[shard] += 1
        log_entry['counter'] = log_count_shards[shard]
        log_entry['type'] = 'delete'
        log_entry['data'] = payload
        log_file_shards[shard].write(json.dumps(log_entry))
        # log_shards[shard] = f"Delete data in {shard} of Stud_id: {sid}"
        
        
        if(is_primary_dict[shard][0] == 1):    
            decision = manage_request_non_primary(shard, payload, '/del', 'DELETE')
            
            if(decision == 'commit'):
                # connection.commit()
                response_json = {
                    "message": f'Data entry with Stud_id:{sid} removed',
                    "status": "success"
                }
                return web.json_response(response_json, status=200)
            else :
                response_json = {
                    "message": 'Delete failed',
                    "status": "failure"
                }
                return web.json_response(response_json, status=404)
        else:
            response_json = {
                "message": f'Acknowledgement of removal of data entry with Stud_id:{sid}',
                "status": "success"
            }
            return web.json_response(response_json, status=200)
        
            
      
      
class Query:
    def __init__(self, query_type, payload):
        self.query_type = query_type
        self.payload = payload
        
    def execute_query(self):
        if(self.query_type == "write"):
            self.write_query()
            print("Write query executed")
        elif(self.query_type == "update"):
            self.update_query()
            print("Update query executed")
        elif(self.query_type == "delete"):
            self.delete_query()
            print("Delete query executed")
        connection.commit()
        print("Query committed")
            
    def write_query(self):
        shard = self.payload.get('shard')
        # curr_idx = int(payload.get('curr_idx'))
        studs_data = self.payload.get('data')
        
        
        for stud in studs_data:
            print(stud)
            sid = int(stud.get('Stud_id'))
            sname = '\"' + stud.get('Stud_name') + '\"'
            smarks = '\"' + stud.get('Stud_marks') + '\"'
            check_query = f"SELECT COUNT(*) FROM {shard} WHERE Stud_id = {sid};"
            cursor.execute(check_query)
            check = cursor.fetchone()[0]
            
            if(check > 0):
                update_query = f"UPDATE {shard} SET Stud_id = {sid}, Stud_name = {sname}, Stud_marks = {smarks} WHERE Stud_id = {sid};"
                cursor.execute(update_query)    
            else:
                write_query = f"INSERT INTO {shard}(Stud_id, Stud_name, Stud_marks) VALUES ({sid}, {sname}, {smarks});"
                cursor.execute(write_query)
                # curr_idx = curr_idx + 1
                
    def update_query(self):
        shard = self.payload.get('shard')
        stud_id = int(self.payload.get('Stud_id'))
        data = self.payload.get('data')
        sid = int(data.get('Stud_id'))
        sname ='\"'+ data.get('Stud_name')+'\"'
        smarks = '\"'+data.get('Stud_marks')+'\"'
            
        # check_query = f"SELECT COUNT(*) FROM {shard} WHERE Stud_id = {sid};"
        # cursor.execute(check_query)
        # check = cursor.fetchone()[0]
        
        # if check <= 0:
        #     return {"error": f"Entry with Stud_id:{sid} does not exist in the given shard"}
        
        update_query = f"UPDATE {shard} SET Stud_id = {sid}, Stud_name = {sname}, Stud_marks = {smarks} WHERE Stud_id = {stud_id};"
        cursor.execute(update_query)
        
    def delete_query(self):
        shard = self.payload.get('shard')
        sid = int(self.payload.get('Stud_id'))
        
        # check_query = f"SELECT COUNT(*) FROM {shard} WHERE Stud_id = {sid};"
        # cursor.execute(check_query)
        # check = cursor.fetchone()[0]
        
        # if check is None:
        #     return {"error": f"Entry with Stud_id:{sid} does not exist in the given shard"}
        
        # if check <= 0:
        #     return {"error": f"Entry with Stud_id:{sid} does not exist in the given shard"}
        
        delete_query = f"DELETE FROM {shard} WHERE Stud_id = {sid};"
        cursor.execute(delete_query)
  


handle_get = Get_handler()
handle_post = Post_handler()
handle_put = Put_handler()
handle_delete = Delete_handler()

app = web.Application()
app.router.add_post('/config', handle_post.config_handler)
app.router.add_post('/read', handle_post.read_handler)
app.router.add_post('/write', handle_post.write_handler)
# app.router.add_post('/updateid', handle_post.updateid_handler)
app.router.add_post('/decision', handle_post.decision_handler)

app.router.add_get('/heartbeat', handle_get.heartbeat_handler)
app.router.add_get('/copy', handle_get.copy_handler)
app.router.add_get('/log_count', handle_get.log_count_handler)
app.router.add_get('/read_all', handle_get.read_all_handler)

app.router.add_put('/update', handle_put.update_handler)
app.router.add_put('/set_primary', handle_put.set_primary)

app.router.add_delete('/del', handle_delete.del_handler)
            

if __name__ == '__main__':
    print('Starting server on port 5000...')
    web.run_app(app, port=5000)