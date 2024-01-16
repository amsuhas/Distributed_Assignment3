import http.client

def send_get_request(host='localhost', port=8000, path='/'):
    connection = http.client.HTTPConnection(host, port)
    connection.request('GET', path)
    
    response = connection.getresponse()
    # print(f'Status: {response.status}')
    # print('Response:')
    # print(response.read().decode('utf-8'))
    
    connection.close()
    return response






from http.server import BaseHTTPRequestHandler, HTTPServer

class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if(self.path == '/rep'):
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'Hello from server!')
            return
        elif(self.path == '/add'):
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            # self.wfile.write(b'This is the about page.')
            return
        elif(self.path == '/rm'):
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            # self.wfile.write(b'This is the about page.')
            return
        else:
            response = send_get_request('localhost', 8001, self.path)
            self.send_response(response.status)
            self.send_header('Content-type', response.getheader('Content-type'))
            self.end_headers()
            self.wfile.write(response.read())
            return
        

def run(server_class=HTTPServer, handler_class=SimpleHTTPRequestHandler, port=8000):
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    print(f'Starting server on port {port}...')
    httpd.serve_forever()

if __name__ == '__main__':
    run()
