"""Weekly player data sync.

Fetches the player list page by page from SOURCE_URL and posts it in
chunks to INGEST_URL, which stores it.

    SOURCE_URL=... INGEST_URL=... INGEST_KEY=... python sync.py
    SOURCE_URL=... python sync.py --probe     # only check the source answers
"""

import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor

from curl_cffi import requests

PAGE_LIMIT = 30
CONCURRENCY = 5
MAX_RETRIES = 4
# Keeps each POST under the ingest's request body limit.
CHUNK_SIZE = 1500


def env(name):
    value = os.environ.get(name)

    if not value:
        raise SystemExit(f'{name} must be set')

    return value


def fetch_page(page):
    url = f"{env('SOURCE_URL')}?action=filter&sort=rating&page={page}&limit={PAGE_LIMIT}"

    for attempt in range(MAX_RETRIES + 1):
        res = requests.get(url, impersonate='chrome', timeout=30)

        if res.status_code == 200:
            return res.json()
        if attempt == MAX_RETRIES:
            raise RuntimeError(f'page {page}: HTTP {res.status_code}')

        time.sleep(attempt + 1)


def crawl():
    first = fetch_page(1)
    total_pages = first.get('totalPages', 1)
    by_id = {p['id']: p for p in first['players']}

    with ThreadPoolExecutor(CONCURRENCY) as pool:
        for data in pool.map(fetch_page, range(2, total_pages + 1)):
            for p in data.get('players', []):
                by_id[p['id']] = p

    return list(by_id.values()), first.get('total', len(by_id))


def post(body):
    res = requests.post(
        env('INGEST_URL'),
        data=json.dumps(body),
        headers={
            'content-type': 'application/json',
            'authorization': f"Bearer {env('INGEST_KEY')}",
        },
        timeout=120,
    )

    if res.status_code != 200:
        raise RuntimeError(f"ingest {body['kind']}: HTTP {res.status_code} {res.text[:300]}")

    return res.json()


def main():
    if '--probe' in sys.argv:
        print(f"Source OK — {fetch_page(1)['total']} players")
        return

    players, reported_total = crawl()
    print(f'Fetched {len(players)}/{reported_total} players')

    for i in range(0, len(players), CHUNK_SIZE):
        result = post({'kind': 'players', 'players': players[i : i + CHUNK_SIZE]})
        print(f"Sent {min(i + CHUNK_SIZE, len(players))}/{len(players)} (stored {result['imported']})")

    result = post({
        'kind': 'finish',
        'ids': [p['id'] for p in players],
        'crawledCount': len(players),
        'reportedTotal': reported_total,
    })
    print('Finished:', json.dumps(result))


main()
