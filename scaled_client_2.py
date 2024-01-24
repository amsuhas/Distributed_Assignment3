import aiohttp
import http.client
import asyncio
import json
import matplotlib.pyplot as plt
import time
import statistics

curr_sum = {}
curr_N = 2
X_axis = []
Y_axis = []

async def send_get_request(session, host='load_balancer', port=5000, path='/home'):
    try:
        async with session.get(f'http://{host}:{port}{path}', timeout=100) as response:
            pass
    except :
        print(f'Request timed out after 100 seconds')

async def main():
    async with aiohttp.ClientSession() as session:
        tasks = [send_get_request(session) for _ in range(10000)]
        await asyncio.gather(*tasks)


def send_add_request(host='load_balancer', port=5000, path='/add'):
    global curr_N
    connection = http.client.HTTPConnection(host, port)
    payload = {"n": 1, "hostnames": []}
    json_data = json.dumps(payload)
    encoded_data = json_data.encode('utf-8')

    headers = {'Content-Type': 'application/json',
           'Content-Length': len(encoded_data)}
    
    connection.request('POST', path, body=encoded_data, headers=headers)
    # connection.request('GET', path)
    response = connection.getresponse()
    print(f'Status: {response.status}')
    print('Response:')
    print(response.read().decode('utf-8'))
    curr_N +=1
    connection.close()

def send_get_request_sync(host='load_balancer', port=5000, path='/info'):
    global curr_sum
    connection = http.client.HTTPConnection(host, port)
    connection.request('GET', path)
    # connection.request('GET', path)
    response = connection.getresponse()
    response = response.read().decode('utf-8')
    counter_dict = json.loads(response)
    hosts = list(counter_dict.keys())
    counters = list(counter_dict.values())
    # print(counter_dict)
    if(len(curr_sum) == 0):
        Y_axis.append(sum(counters)/len(counters))
        # print(counters)
        # Y_axis.append(statistics.stdev(counters))
    else:
        result = [a - b if i < len(curr_sum) else a for i, (a, b) in enumerate(zip(counters, curr_sum))]
        result.extend(counters[len(curr_sum):])
        Y_axis.append(sum(result)/len(result))
        # print(curr_sum)
        # print(counters)
        # print(result)
        # Y_axis.append(statistics.stdev(result))
    curr_sum = counters
    connection.close()

if __name__ == '__main__':

    # N = 2
    X_axis.append(curr_N)    
    asyncio.run(main())
    time.sleep(20)
    send_get_request_sync()
    send_add_request()

    # N = 3
    X_axis.append(curr_N)    
    asyncio.run(main())
    time.sleep(20)
    send_get_request_sync()
    send_add_request()

    # N = 4
    X_axis.append(curr_N)    
    asyncio.run(main())
    time.sleep(20)
    send_get_request_sync()
    send_add_request()

    # N = 5
    X_axis.append(curr_N)    
    asyncio.run(main())
    time.sleep(20)
    send_get_request_sync()
    send_add_request()

    # N = 6
    X_axis.append(curr_N)    
    asyncio.run(main())
    time.sleep(20)
    send_get_request_sync()

    plt.plot(X_axis, Y_axis, label='Line Chart', marker='o', linestyle='-', color='b')
    plt.xlabel('X-axis Label')
    plt.ylabel('Y-axis Label')
    plt.title('Line Chart Example')
    plt.savefig('line_chart.png')


