import aiohttp
import http.client
import asyncio
import json
import matplotlib.pyplot as plt
import time


async def send_get_request(session, host='load_balancer', port=5000, path='/home'):
    # payload = {"n": 1, "hostnames": []}
    # json_data = json.dumps(payload)
    # headers = {'Content-Type': 'application/json'}

    try:
        async with session.get(f'http://{host}:{port}{path}', timeout=100) as response:
            pass
    except :
        print(f'Request timed out after 100 seconds')

async def main():
    async with aiohttp.ClientSession() as session:
        tasks = [send_get_request(session) for _ in range(10000)]
        await asyncio.gather(*tasks)


def send_get_request_sync(host='load_balancer', port=5000, path='/info'):
    connection = http.client.HTTPConnection(host, port)
    connection.request('GET', path)
    # connection.request('GET', path)
    response = connection.getresponse()
    response = response.read().decode('utf-8')
    counter_dict = json.loads(response)
    hosts = list(counter_dict.keys())
    print(hosts)
    counters = list(counter_dict.values())

    # Plotting the bar graph
    plt.bar(hosts, counters, color='blue')
    plt.xlabel('Host Names')
    plt.ylabel('Counter Values')
    plt.title('Counter Values for Hosts')
    plt.savefig('bar_graph.png')
    plt.show()
    connection.close()

if __name__ == '__main__':
    asyncio.run(main())
    # time.sleep(20)
    send_get_request_sync()
