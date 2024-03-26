import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer
import mysql.connector
import copy

server_id = os.environ.get('ID')

connection = mysql.connector.connect(
    host="localhost",
    user="myuser",
    password="mypass",
    database="Student_info"
)

cursor = connection.cursor()

update_idx_dict = {}

class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):

        if(self.path == '/heartbeat'):
            print("Received heartbeat request\n")
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            return
        elif self.path == '/copy':
            print("Received copy request\n")
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            payload = json.loads(post_data)
            
            shards = payload.get('shards')


            response_data = {}

            for shard in shards:
                query = f"SELECT * FROM {shard} LIMIT {update_idx_dict[shard]};"
                cursor.execute(query)
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

            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response_data["status"] = "success"
            print(response_data)
            self.wfile.write(json.dumps(response_data).encode('utf-8'))
            return
    
    def do_POST(self):
        if self.path == '/config':
            print("Received config request\n")
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            payload = json.loads(post_data)

            schema = payload.get('schema')
            shards = payload.get('shards')

            tables_created = ""
            dict={}
            dict['Number'] = 'INT'
            dict['String'] = 'VARCHAR(255)'
            for shard in shards:
                update_idx_dict[shard] = 0
                table_name = shard
                columns = ', '.join([f"{col} {dict[dtype]}" for col, dtype in zip(schema['columns'], schema['dtypes'])])
                print(columns)
                create_table_query = f"CREATE TABLE {table_name} ({columns});"

                print(create_table_query)
                cursor.execute(create_table_query)
                connection.commit()
                tables_created += (f"{server_id}:{table_name}, ")
            tables_created =  tables_created[:-2]
            tables_created += (" configured")

            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response_json = {
                "message": tables_created,
                "status": "success"
            }
            self.wfile.write(json.dumps(response_json).encode('utf-8'))
            return
        
        elif self.path == '/read':
            print("Received read request\n")
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            payload = json.loads(post_data)

            shard = payload.get('shard')
            stud_id_range = payload.get('Stud_id')


            low = int(stud_id_range.get('low'))
            high = int(stud_id_range.get('high'))


            response_data = {}

            query = f"SELECT * FROM (SELECT * FROM {shard} LIMIT {update_idx_dict[shard]}) AS subquery WHERE Stud_id BETWEEN {low} AND {high};"
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


            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response_data["status"] = "success"
            self.wfile.write(json.dumps(response_data).encode('utf-8'))
            return
        
        elif self.path == '/write':
            print("Received write request\n")
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            payload = json.loads(post_data)
            

            shard = payload.get('shard')
            curr_idx = int(payload.get('curr_idx'))
            studs_data = payload.get('data')
            
            
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
                    curr_idx = curr_idx + 1
            connection.commit()
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response_json = {
                "message": 'Data entries added',
                "current_idx": curr_idx,
                "status": "success"
            }
            self.wfile.write(json.dumps(response_json).encode('utf-8'))
            return
        
        elif self.path == '/updateid':
            print("Received update index request\n")
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            payload = json.loads(post_data)
            
            shard = payload.get('shard')
            update_idx_dict[shard] = int(payload.get('update_idx'))
            
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response_json = {
                "message": 'Update index updated',
                "status": "success"
            }
            self.wfile.write(json.dumps(response_json).encode('utf-8'))
            return            
        
    def do_PUT(self):
        if self.path == '/update':
            print("Received update request\n")
            content_length = int(self.headers['Content-Length'])
            put_data = self.rfile.read(content_length)
            payload = json.loads(put_data)
            
            shard = payload.get('shard')
            stud_id = int(payload.get('Stud_id'))
            data = payload.get('data')
            sid = int(data.get('Stud_id'))
            sname ='\"'+ data.get('Stud_name')+'\"'
            smarks = '\"'+data.get('Stud_marks')+'\"'
             
            
            check_query = f"SELECT COUNT(*) FROM {shard} WHERE Stud_id = {sid};"
            cursor.execute(check_query)
            check = cursor.fetchone()[0]
            
            if check <= 0:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(json.dumps({"error": f"Entry with Stud_id:{stud_id} does not exist in the given shard"}).encode('utf-8'))
                return
            
            update_query = f"UPDATE {shard} SET Stud_id = {sid}, Stud_name = {sname}, Stud_marks = {smarks} WHERE Stud_id = {stud_id};"
            cursor.execute(update_query)
            connection.commit()
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response_json = {
                "message": f'Data entry for Stud_id:{stud_id} updated',
                "status": "success"
            }
            self.wfile.write(json.dumps(response_json).encode('utf-8'))
            return
        
    def do_DELETE(self):
        if self.path == '/del':
            print("Received delete request\n")
            content_length = int(self.headers['Content-Length'])
            delete_data = self.rfile.read(content_length)
            payload = json.loads(delete_data)
            
            shard = payload.get('shard')
            sid = int(payload.get('Stud_id'))
            
            
            check_query = f"SELECT COUNT(*) FROM {shard} WHERE Stud_id = {sid};"
            cursor.execute(check_query)
            check = cursor.fetchone()[0]
            
            if check is None:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(json.dumps({"error": f"Entry with Stud_id:{sid} does not exist in the given shard"}).encode('utf-8'))
                return
            
            if check <= 0:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(json.dumps({"error": f"Entry with Stud_id:{sid} does not exist in the given shard"}).encode('utf-8'))
                return
            
            delete_query = f"DELETE FROM {shard} WHERE Stud_id = {sid};"
            cursor.execute(delete_query)
            connection.commit()
            update_idx_dict[shard] = update_idx_dict[shard] - 1

            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response_json = {
                "message": f'Data entry with Stud_id:{sid} removed',
                "status": "success"
            }
            self.wfile.write(json.dumps(response_json).encode('utf-8'))
            return
            
        
            
        

def run(server_class=HTTPServer, handler_class=SimpleHTTPRequestHandler, port=5000):
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    print(f'Starting server on port {port}...')
    httpd.serve_forever()

if __name__ == '__main__':
    run()
