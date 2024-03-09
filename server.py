# server.py
import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer
import mysql.connector

server_id = os.environ.get('ID')

connection = mysql.connector.connect(
    host="localhost",
    user="myuser",
    password="my",
    database="Student_info"
)

cursor = connection.cursor()   

class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        # if(self.path == '/home'):
        #     print("Received home request\n")
        #     self.send_response(200)
        #     self.send_header('Content-type', 'application/json')
        #     self.end_headers()
        #     server_response = {"text": "Hello from server " + server_id + "!"}
        #     response_str = json.dumps(server_response)
        #     self.wfile.write(response_str.encode('utf-8'))
        #     return
        if(self.path == '/heartbeat'):
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            return
        elif self.path == '/copy':
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            payload = json.loads(post_data)

            shards = payload.get('shards')

            # if shards is None:
            #     self.send_response(400)
            #     self.end_headers()
            #     self.wfile.write(json.dumps({"error": "Shards missing in the request payload"}).encode('utf-8'))
            #     return

            # Fetch data from MySQL tables corresponding to the shards
            response_data = {}
            # try:
            # Iterate over shards
            for shard in shards:
                # Check if the table exists in the database
                # table_exists_query = f"SELECT COUNT(*) FROM Student_info.tables WHERE table_schema = '{connection.database}' AND table_name = '{shard}'"
                # cursor.execute(table_exists_query)
                # table_exists = cursor.fetchone()['COUNT(*)']

                # if table_exists:
                #     # Fetch data from corresponding MySQL table
                query = f"SELECT * FROM {shard};"
                cursor.execute(query)
                rows = cursor.fetchall()
                res = ""
                for row in rows:
                    res += "{" + '\"' + "Stud_id" + '\"' + ":" + str(row[0]) + "," + '\"' + "Stud_name" + '\"' + ":" + '\"' + row[1] + '\"' + "," + '\"' + "Stud_marks" + '\"' + ":"  + row[2] + "},\n"
                response_data["data"] = "[" + res[:-2] + "]"

                # Append data to response dictionary
                # response_data[shard] = rows
                # else:
                #     response_data[shard] = f"Table '{shard}' does not exist"
            # except Exception as e:
            #     print(f"Error: {e}")
            #     response_data = {"error": str(e)}

            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response_data["status"] = "success"
            self.wfile.write(json.dumps(response_data).encode('utf-8'))
            return
        # else:
        #     self.send_response(404)
        #     self.send_header('Content-type', 'text/plain')
        #     self.end_headers()
        #     self.wfile.write(b'404 Not Found')
        #     return
    
    def do_POST(self):
        if self.path == '/config':
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            payload = json.loads(post_data)

            schema = payload.get('schema')
            shards = payload.get('shards')

            # if schema is None or shards is None:
            #     self.send_response(400)
            #     self.end_headers()
            #     self.wfile.write(json.dumps({"error": "Schema or shards missing in the request payload"}).encode('utf-8'))
            #     return

            # Create tables for each shard
            tables_created = ""
            dict={}
            dict['Number'] = 'INT'
            dict['String'] = 'VARCHAR(255)'
            for shard in shards:
                table_name = shard
                columns = ', '.join([f"{col} {dict[dtype]}" for col, dtype in zip(schema['columns'], schema['dtypes'])])
                print(columns)
                create_table_query = f"CREATE TABLE {table_name} ({columns});"
                # Execute the create table query in your database
                # Assuming you have established a connection and cursor
                # Example:
                print(create_table_query)
                cursor.execute(create_table_query)
                connection.commit()
                tables_created += (f"{server_id}:{table_name}, ")
            tables_created =  tables_created[:-2]
            tables_created += (" configured")
            # Send response
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response_json = {
                "message": ', '.join(tables_created),
                "status": "success"
            }
            self.wfile.write(json.dumps(response_json).encode('utf-8'))
            return
        
        elif self.path == '/read':
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            payload = json.loads(post_data)

            shard = payload.get('shard')
            stud_id_range = payload.get('Stud_id')

            # if shard is None or stud_id_range is None:
            #     self.send_response(400)
            #     self.end_headers()
            #     self.wfile.write(json.dumps({"error": "Shard or Stud_id range missing in the request payload"}).encode('utf-8'))
            #     return
            

            # if low is not None and high is not None:
            low = int(stud_id_range.get('low'))
            high = int(stud_id_range.get('high'))

            # if low is None or high is None:
            #     self.send_response(400)
            #     self.end_headers()
            #     self.wfile.write(json.dumps({"error": "Low or High value missing in the Stud_id range"}).encode('utf-8'))
            #     return

            # Fetch data from MySQL table corresponding to the shard and stud_id range
            response_data = {}
            # try:
            query = f"SELECT * FROM {shard} WHERE Stud_id BETWEEN {low} AND {high};"
            cursor.execute(query)
            rows = cursor.fetchall()

            # Append data to response dictionary
            out_list = []
            res = ""
            for row in rows:
                res += "{" + '"' + "Stud_id" + '"' + ":" + str(row[0]) + "," + '"' + "Stud_name" + '"' + ":" + '"' + row[1] + '"' + "," + '"' + "Stud_marks" + '"' + ":"  + row[2]  + "}"
                out_list.append(res)
            response_data["data"] = out_list
            print(response_data)
            print(json.dumps(response_data))
            # except Exception as e:
            #     print(f"Error: {e}")
            #     response_data = {"error": str(e)}

            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response_data["status"] = "success"
            self.wfile.write(json.dumps(response_data).encode('utf-8'))
            return
        
        elif self.path == '/write':
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            payload = json.loads(post_data)
            
            shard = payload.get('shard')
            curr_idx = int(payload.get('curr_idx'))
            studs_data = payload.get('data')
            
            # if shard is None or curr_idx is None:
            #     self.send_response(400)
            #     self.end_headers()
            #     self.wfile.write(json.dumps({"error": "Shard or curr_idx missing in the request payload"}).encode('utf-8'))
            #     return
            
            for stud in studs_data:
                sid = int(stud.get('Stud_id'))
                check_query = f"SELECT COUNT(*) FROM {shard} WHERE Stud_id = {sid};"
                cursor.execute(check_query)
                check = cursor.fetchone()[0]
                
                if check > 0:
                    self.send_response(400)
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": f"Entry with Stud_id:{sid} already exists in the given shard"}).encode('utf-8'))
                    return
            
            for stud in studs_data:
                print(stud)
                sid = int(stud.get('Stud_id'))
                sname = '\"' + stud.get('Stud_name') + '\"'
                smarks = '\"' + stud.get('Stud_marks') + '\"'
                write_query = f"INSERT INTO {shard}(Stud_id, Stud_name, Stud_marks) VALUES ({sid}, {sname}, {smarks});"
                print(write_query)
                cursor.execute(write_query)
                curr_idx = curr_idx + 1
                
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
        
    def do_PUT(self):
        if self.path == '/update':
            content_length = int(self.headers['Content-Length'])
            put_data = self.rfile.read(content_length)
            payload = json.loads(put_data)
            
            shard = payload.get('shard')
            stud_id = int(payload.get('Stud_id'))
            data = payload.get('data')
            sid = int(data.get('Stud_id'))
            sname ='\"'+ data.get('Stud_name')+'\"'
            smarks = '\"'+data.get('Stud_marks')+'\"'
             
            # if shard is None or stud_id is None:
            #     self.send_response(400)
            #     self.end_headers()
            #     self.wfile.write(json.dumps({"error": "Shard or Stud_id is missing in the request payload"}).encode('utf-8'))
            #     return
            
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
            content_length = int(self.headers['Content-Length'])
            delete_data = self.rfile.read(content_length)
            payload = json.loads(delete_data)
            
            shard = payload.get('shard')
            sid = int(payload.get('Stud_id'))
            
            # if shard is None or sid is None:
            #     self.send_response(400)
            #     self.end_headers()
            #     self.wfile.write(json.dumps({"error": "Shard or Stud_id is missing in the request payload"}).encode('utf-8'))
            #     return
            
            check_query = f"SELECT COUNT(*) FROM {shard} WHERE Stud_id = {sid};"
            cursor.execute(check_query)
            check = cursor.fetchone()[0]
            
            if check <= 0:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(json.dumps({"error": f"Entry with Stud_id:{sid} does not exist in the given shard"}).encode('utf-8'))
                return
            
            delete_query = f"DELETE FROM {shard} WHERE Stud_id = {sid};"
            cursor.execute(delete_query)
            
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