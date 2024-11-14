# -*- coding: utf-8 -*-
import logging
import asyncio
from aiohttp.web import HTTPError, HTTPException
from aiohttp.client_exceptions import ClientError
from typing import Callable, Awaitable

# local imports
from config import *


# Configure logging
logging.basicConfig(format='%(levelname)-8s [%(asctime)s] %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)


async def http_request(func: Callable, url: str, headers: dict, params: dict = None, data=None, json=None,
                       result_json=True, result_binary=False,
                       retries=RETRY_COUNT, check_key=None):
    for _ in range(retries):
        try:
            async with func(url, headers=headers, params=params, data=data, json=json) as resp:
                if not resp.ok:
                    raise HTTPError(reason=resp.reason)
                if result_binary:
                    body_bytes = await resp.read()
                    return body_bytes
                elif result_json:
                    result = await resp.json()
                    if check_key and (check_key not in result or result[check_key] is None):
                        raise HTTPError(reason=f'required key {check_key} not in API result or is None')
                    return result
                else:
                    body_bytes = await resp.read()
                    return body_bytes.decode('utf-8')

        except (HTTPException, ClientError) as e:
            log.info(f'HTTP request error: {e}, waiting {RETRY_PAUSE} seconds and retrying...')
            await asyncio.sleep(RETRY_PAUSE)

    raise RuntimeError(f'HTTP request failed after {retries} tries, giving up')


async def get_list_stable(
        func: Callable[[], Awaitable[list]],
        callback: Callable[[list, float, bool, str], None],
        chunk: int,
        max_tries: int,
        retry_pause: float,
        unique_key: str = None,
        tolerance: int = 0
) -> list:
    hist, max_number = [], 0
    for _ in range(max_tries):
        objects = await func()
        if unique_key and len(objects) > len({o[unique_key] for o in objects}):
            descr = f'got {len(objects)}, duplicates found, waiting {retry_pause}s and retry...'
            callback(objects, retry_pause, True, descr)
            await asyncio.sleep(retry_pause)
            continue
        hist.append(objects)
        if len(objects) > max_number:
            max_number = len(objects)
        if len(hist) >= chunk and all(max_number - len(o) <= tolerance for o in hist[-chunk:]):
            callback(objects, 0, False, f'got {len(objects)}, stable')
            return objects
        callback(objects, retry_pause, False, f'got {len(objects)}, waiting {retry_pause}s and retry...')
        await asyncio.sleep(retry_pause)

    raise RuntimeError('failed to get the stable list of objects')
