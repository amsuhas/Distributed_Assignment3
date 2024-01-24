import aiohttp
import asyncio
import json

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
        tasks = [send_get_request(session) for _ in range(1000)]
        await asyncio.gather(*tasks)

if __name__ == '__main__':
    asyncio.run(main())