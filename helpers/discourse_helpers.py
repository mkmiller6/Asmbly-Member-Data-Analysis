import asyncio
import aiohttp

from config import D_APIkey, D_APIuser
from helpers.api_exponential_backoff import backoff_time

D_BASE_URL = "https://yo.asmbly.org"
D_HEADERS = {
    "Content-Type": "application/json",
    "Api-Key": D_APIkey,
    "Api-Username": D_APIuser,
}


async def get_discourse_data(aio_session: aiohttp.ClientSession, discourse_id: int):

    resource_path = f"/u/{discourse_id}.json"
    max_retries = 10
    url = D_BASE_URL + resource_path

    for i in range(max_retries):
        async with aio_session.get(url, headers=D_HEADERS) as response:
            if response.status == 200:
                data = await response.json()
                break
            if response.status in set([429, 502]):
                await asyncio.sleep(backoff_time(i))
            else:
                print(response.status)
                return None

    posts = data.get("user")["post_count"]
    time_read = data.get("user")["time_read"]

    discourse_info = {"posts": posts, "reading time": time_read}

    return discourse_info
