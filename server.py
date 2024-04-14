import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
import mysql.connector
import copy
import asyncio
from aiohttp import web

my_server_id = os.environ.get('ID')
connection = {}
cursor = {}
read_connection = mysql.connector.connect(
    host="localhost",
    user="myuser",
    password="mypass",
    # database="Student_info"
)
read_cursor = read_connection.cursor()

# Create a connection pool
connection_metadata = mysql.connector.pooling.MySQLConnectionPool(
    host="lb_database",
    user="root",
    password="password"
)

# cursor = connection.cursor()
# update_idx_dict = {}
# primary_shards = []
log_shards = {}
log_count_shards = {}
logs_file = open("logs.txt", "w")
is_primary_dict = {}

class Metadata:
    def __init__(self):
        connection = connection_metadata.get_connection()
        cursor = connection.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS Metadata")
        cursor.execute("USE Metadata")

        # print("Creating tables")
        # table_name = "ShardT"
        # create_table_query = f"CREATE TABLE {table_name} ( Stud_id_low INT, Shard_id INT, Shard_size INT, Valid_idx INT, Update_idx INT);"
        # print(create_table_query)
        # cursor.execute(create_table_query)

        # table_name = "MapT"
        # create_table_query = f"CREATE TABLE {table_name} ( Shard_id INT, Server_id INT);"
        # print(create_table_query)
        # cursor.execute(create_table_query)
        connection.commit()
        cursor.close()
        connection.close()

    def add_shard(self, shard_id, shard_size,shard_id_low):
        connection = connection_metadata.get_connection()
        cursor = connection.cursor()
        cursor.execute("USE Metadata")
        print(f"Inside add_shard function. adding {shard_id} info to ShardT")
        insert_query = f"INSERT INTO ShardT (Stud_id_low, Shard_id, Shard_size, Valid_idx, Update_idx) VALUES ({shard_id_low}, {shard_id}, {shard_size}, 0, -1);"
        cursor.execute(insert_query)
        connection.commit()    
        cursor.close() 
        connection.close()

    def remove_shard(self, shard_id):
        connection = connection_metadata.get_connection()
        cursor = connection.cursor()
        cursor.execute("USE Metadata")
        print(f"Removing shard:{shard_id} from ShardT")
        delete_query = f"DELETE FROM ShardT WHERE Shard_id = {shard_id};"
        cursor.execute(delete_query)
        connection.commit()
        cursor.close()
        connection.close()
    
    def add_server(self, server_id, shard_list):
        connection = connection_metadata.get_connection()
        cursor = connection.cursor()
        cursor.execute("USE Metadata")
        print(f"adding server:{server_id} having shard_list: {shard_list} in MapT")
        for shard in shard_list:
            insert_query = f"INSERT INTO MapT (Shard_id, Server_id) VALUES ({shard}, {server_id});"
            cursor.execute(insert_query)
        connection.commit()
        cursor.close()
        connection.close()

    def remove_server(self, server_id):
        connection = connection_metadata.get_connection()
        cursor = connection.cursor()
        cursor.execute("USE Metadata")
        print(f"Removing server:{server_id} from MapT")
        delete_query = f"DELETE FROM MapT WHERE Server_id = {server_id};"
        cursor.execute(delete_query)
        connection.commit()
        cursor.close()
        connection.close()

    def get_shards(self, server_id):
        connection = connection_metadata.get_connection()
        cursor = connection.cursor()
        cursor.execute("USE Metadata")
        print(f"Getting shards for server:{server_id}")
        select_query = f"SELECT Shard_id FROM MapT WHERE Server_id = {server_id};"
        cursor.execute(select_query)
        shard_list = cursor.fetchall()
        cursor.close()
        connection.close()
        return shard_list

    def get_all_shards(self):
        connection = connection_metadata.get_connection()
        cursor = connection.cursor()
        cursor.execute("USE Metadata")
        print(f"Getting all shards")
        select_query = f"SELECT Stud_id_low, Shard_id, Shard_size FROM ShardT;"
        cursor.execute(select_query)
        shard_list = cursor.fetchall()
        cursor.close()
        connection.close()
        return shard_list
    
    def get_shard_id(self, stud_id):
        connection = connection_metadata.get_connection()
        cursor = connection.cursor()
        cursor.execute("USE Metadata")
        print(f"Getting shard_id for stud_id:{stud_id}")
        select_query = f"SELECT Shard_id FROM ShardT WHERE Stud_id_low <= {stud_id} AND Stud_id_low + Shard_size > {stud_id};"
        cursor.execute(select_query)
        shard_id = cursor.fetchall()
        cursor.close()
        connection.close()
        if len(shard_id) == 0:
            return None
        return shard_id[0][0]
    
    def get_server_id(self, shard_id):
        connection = connection_metadata.get_connection()
        cursor = connection.cursor()
        cursor.execute("USE Metadata")
        print(f"Getting server_id for shard_id:{shard_id}")
        select_query = f"SELECT Server_id FROM MapT WHERE Shard_id = {shard_id};"
        cursor.execute(select_query)
        server_ids = cursor.fetchall()
        cursor.close()
        connection.close()
        return server_ids
    
    def get_valid_idx(self, shard_id):
        connection = connection_metadata.get_connection()
        cursor = connection.cursor()
        cursor.execute("USE Metadata")
        print(f"Getting valid_idx for shard_id:{shard_id}")
        select_query = f"SELECT Valid_idx FROM ShardT WHERE Shard_id = {shard_id};"
        cursor.execute(select_query)
        valid_idx = cursor.fetchall()
        cursor.close()
        connection.close()
        return valid_idx[0][0]
    
    def get_update_idx(self, shard_id):
        connection = connection_metadata.get_connection()
        cursor = connection.cursor()
        cursor.execute("USE Metadata")
        print(f"Getting update_idx for shard_id:{shard_id}")
        select_query = f"SELECT Update_idx FROM ShardT WHERE Shard_id = {shard_id};"
        cursor.execute(select_query)
        update_idx = cursor.fetchall()
        cursor.close()
        connection.close()
        return update_idx[0][0]
    
    def update_valid_idx(self, shard_id, valid_idx):
        connection = connection_metadata.get_connection()
        cursor = connection.cursor()
        cursor.execute("USE Metadata")
        print(f"Updating valid_idx for shard_id:{shard_id} to {valid_idx}")
        update_query = f"UPDATE ShardT SET Valid_idx = {valid_idx} WHERE Shard_id = {shard_id};"
        cursor.execute(update_query)
        connection.commit()
        cursor.close()
        connection.close()

    def update_update_idx(self, shard_id, update_idx):
        connection = connection_metadata.get_connection()
        cursor = connection.cursor()
        cursor.execute("USE Metadata")
        print(f"Updating update_idx for shard_id:{shard_id} to {update_idx}")
        update_query = f"UPDATE ShardT SET Update_idx = {update_idx} WHERE Shard_id = {shard_id};"
        cursor.execute(update_query)
        connection.commit()
        cursor.close()
        connection.close()
        
        
        
        
metadata_obj = Metadata()   


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
            if(server_id == my_server_id):
                continue
            server_name = "server" + str(server_id)
            try:
                response, status_code = send_request(server_name, 5000, endpoint, payload, method)
                if response['status'] == "success":
                    success_count += 1
            except:
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
            
        for server_id in server_ids:
            try:
                response, status = server_decision(shard, decision, server_id)
            except:
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
            use_query = f"USE {shard};"
            read_cursor.execute(use_query)
            query = f"SELECT * FROM {shard};"
            read_cursor.execute(query)
            
            # note 
            rows = read_cursor.fetchall()
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
        


    
class Post_handler:
    async def config_handler(self, request):
        print("Received config request\n")
        
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
        for shard in shards:
            
            connection[shard] = mysql.connector.connect(
                host="localhost",
                user="myuser",
                password="mypass",
                # database="Student_info"
            )
            cursor[shard] = connection[shard].cursor()
            
            log_count_shards[shard] = 0
            is_primary_dict[shard] = (0, [])
            
            # update_idx_dict[shard] = 0
            # table_name = shard
            # columns = ', '.join([f"{col} {dict[dtype]}" for col, dtype in zip(schema['columns'], schema['dtypes'])])
            # # print(columns)
            # create_table_query = f"CREATE TABLE {table_name} ({columns});"
            
            database_name = shard
            create_database_query = f"CREATE DATABASE IF NOT EXISTS {database_name};"
            
            table_name = shard
            columns = ', '.join([f"{col} {dict[dtype]}" for col, dtype in zip(schema['columns'], schema['dtypes'])])
            # print(columns)
            create_table_query = f"CREATE TABLE {table_name} ({columns});"
            

            # print(create_database_query)
            # print(create_table_query)
            cursor[shard].execute(create_database_query)
            cursor[shard].execute(create_table_query)
            connection[shard].commit()
            tables_created += (f"{my_server_id}:{database_name}, ")
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

        use_query = f"USE {shard};"
        read_cursor.execute(use_query)

        query = f"SELECT * FROM {shard} WHERE Stud_id BETWEEN {low} AND {high};"
        read_cursor.execute(query)
        rows = read_cursor.fetchall()


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
            sid = int(stud.get('Stud_id'))
            sname = '\"' + stud.get('Stud_name') + '\"'
            smarks = '\"' + stud.get('Stud_marks') + '\"'
            
            use_query = f"USE {shard};"
            cursor[shard].execute(use_query)
        
            check_query = f"SELECT COUNT(*) FROM {shard} WHERE Stud_id = {sid};"
            cursor[shard].execute(check_query)
            check = cursor[shard].fetchone()[0]
            
            if(check > 0):
                update_query = f"UPDATE {shard} SET Stud_id = {sid}, Stud_name = {sname}, Stud_marks = {smarks} WHERE Stud_id = {sid};"
                cursor[shard].execute(update_query)    
            else:
                write_query = f"INSERT INTO {shard}(Stud_id, Stud_name, Stud_marks) VALUES ({sid}, {sname}, {smarks});"
                cursor[shard].execute(write_query)
                # curr_idx = curr_idx + 1
        
        log_shards[shard] = f"writing data in {shard}: \n{studs_data}"
            
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
        shard = payload.get('shard')
        decision = payload.get('decision')
        if decision == "commit":
            connection[shard].commit()
            response_json = {
                "message": f'{shard} database committed',
                "status": "success"
            }
            log_count_shards[shard] += 1
            logs_file.write(log_shards[shard])
            logs_file.write("\n")
            logs_file.flush()
            return web.json_response(response_json, status=200)
        else:
            connection[shard].rollback()
            response_json = {
                "message": f'{shard} database rolled back',
                "status": "success"
            }
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
            
        use_query = f"USE {shard};"
        cursor[shard].execute(use_query)
        
        check_query = f"SELECT COUNT(*) FROM {shard} WHERE Stud_id = {sid};"
        cursor[shard].execute(check_query)
        check = cursor[shard].fetchone()[0]
        
        if check <= 0:
            return web.json_response({"error": f"Entry with Stud_id:{sid} does not exist in the given shard"}, status=400)
        
        update_query = f"UPDATE {shard} SET Stud_id = {sid}, Stud_name = {sname}, Stud_marks = {smarks} WHERE Stud_id = {stud_id};"
        cursor[shard].execute(update_query)
        # connection[shard].commit()
        # response_json = {
        #     "message": f'Data entry for Stud_id:{stud_id} updated',
        #     "status": "success"
        # }
        # return web.json_response(response_json, status=200)
        
        
        log_shards[shard] = f"Updating data in {shard} for Stud_id: {stud_id} with data:\n{data}"
            
        
            
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
        
        
    
    async def set_primary(self, request):
        print("Received set primary request\n")
        payload = await request.json()
        
        shard = payload.get('shard')
        secondary_servers = payload.get('secondary_servers')
        # for shard in shards:
            # primary_shards.append(shard)
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
        
        use_query = f"USE {shard};"
        cursor[shard].execute(use_query)
        
        check_query = f"SELECT COUNT(*) FROM {shard} WHERE Stud_id = {sid};"
        cursor[shard].execute(check_query)
        check = cursor[shard].fetchone()[0]
        
        if check is None:
            return web.json_response({"error": f"Entry with Stud_id:{sid} does not exist in the given shard"}, status=400)
        
        if check <= 0:
            return web.json_response({"error": f"Entry with Stud_id:{sid} does not exist in the given shard"}, status=400)
        
        delete_query = f"DELETE FROM {shard} WHERE Stud_id = {sid};"
        cursor[shard].execute(delete_query)
        # connection[shard].commit()
        # update_idx_dict[shard] = update_idx_dict[shard] - 1
        
        log_shards[shard] = f"Delete data in {shard} of Stud_id: {sid}"
            
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

app.router.add_put('/update', handle_put.update_handler)
app.router.add_put('/set_primary', handle_put.set_primary)

app.router.add_delete('/del', handle_delete.del_handler)
            

if __name__ == '__main__':
    print('Starting server on port 5000...')
    web.run_app(app, port=5000)