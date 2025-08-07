import argparse
import asyncio
import json
import math
import multiprocessing
import sys
import uuid
from multiprocessing.managers import ListProxy
from time import sleep

from elastic_transport import ObjectApiResponse
from elasticsearch import Elasticsearch, helpers, AsyncElasticsearch

from utils.benchmark import timeit
from utils.env import check_is_prod, get_es_url
from utils.es import created_adjacency_list_index
from utils.constants import EDGE_INDEX, ADJ_INDEX
from utils.parallel import get_n_workers


def clean_slate(es_url: str):
    migrate = created_adjacency_list_index(es_url)

    # migrate data from nodes to adj_list as a base
    migrate()


def get_run_id():
    return uuid.uuid4().hex[:10]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch", type=int, help="Index of the batch to process")

    args = parser.parse_args()

    is_prod = check_is_prod()

    es_url = get_es_url()
    # clean_slate(es_url)
    run_id = get_run_id()

    total_workers = get_n_workers()
    # how many async actions allowed per worker
    concurrency_limit = 5
    progress_array = multiprocessing.Array('i', [0] * total_workers)

    limit = 10000
    # limit = 1500

    # node_id_file = './10k_nodes_id.json'
    node_id_file = './nodes_id.json'


    if args.batch is not None:
        node_ids = get_node_ids_for_batch(node_id_file, args.batch)
    else:
        node_ids = get_node_ids(node_id_file, limit)

    total_nodes = len(node_ids)
    nodes_per_worker = math.ceil(total_nodes / total_workers)

    # Start monitor
    monitor_proc = multiprocessing.Process(target=monitor_progress, args=(progress_array, total_nodes, run_id))
    monitor_proc.start()

    with multiprocessing.Manager() as manager, timeit(f'process {total_nodes} nodes'):
        # create container for failed nodes
        failed_nodes = manager.list()

        # Spawn workers
        workers = []
        for i in range(total_workers):
            start = i * nodes_per_worker
            end = min(start + nodes_per_worker, total_nodes)
            chunk = node_ids[start:end]

            p = multiprocessing.Process(target=run_per_worker, args=(es_url, concurrency_limit, chunk, progress_array, failed_nodes, i))
            p.start()
            workers.append(p)

        # Wait for all workers to finish
        for p in workers:
            p.join()

        if failed_nodes:
            print(f'{len(failed_nodes)} nodes failed')
            write_failed_nodes(failed_nodes, run_id)


    monitor_proc.join()

def write_failed_nodes(failed_nodes: ListProxy, run_id: str):
    with open(f'./failed_nodes_{run_id}.json', 'w', encoding='utf-8') as f:
        json.dump(list(failed_nodes), f)

def get_node_ids_for_batch(target_file: str, batch_index: int, num_of_batches=5):
    assert 0 <= batch_index < num_of_batches

    with open(target_file, "rb") as f:
        full_ids = json.load(f)

    total_len = len(full_ids)
    k, m = divmod(total_len, num_of_batches)
    start = batch_index * k + min(batch_index, m)
    end = start + k + (1 if batch_index < m else 0)

    return full_ids[start:end]


def get_node_ids(target_file: str, limit: int | None):
    with open(target_file, "rb") as f:
        full_ids = json.load(f)


    if limit is None:
        return full_ids


    # only use partial ids for testing
    node_ids = full_ids[:limit]
    del full_ids

    return node_ids


def monitor_progress(progress_array, total_count: int, run_id: str):
    while True:
        sleep(1)
        current_total = sum(progress_array)
        print(f"Run {run_id} progress: {current_total}/{total_count} nodes processed", end='\r', flush=True)
        if current_total >= total_count:
            print()  # newline after complete
            break

async def generate_actions(es_client: AsyncElasticsearch, concurrency_limit: int, nodes_ids: list[str], progress_array: list, failed_nodes: list, worker_id:int):
    semaphore = asyncio.Semaphore(concurrency_limit)

    async def process_with_semaphore(_node_id: str):
        async with semaphore:
            payload, total_edges = await process_single_node(es_client, _node_id)
            progress_array[worker_id] += 1
            return payload, _node_id

    tasks_generator = (process_with_semaphore(node_id) for node_id in nodes_ids)

    for coro in asyncio.as_completed(tasks_generator):
        payload, node_id = await coro
        if payload is not None:
            yield payload
        else:
            print(f'something wrong with {node_id}')
            failed_nodes.append(node_id)


# async wrapper for worker task
def run_per_worker(*args):
    asyncio.run(per_worker(*args))

# entry point for paral. work
async def per_worker(es_url:str, *args):
    async_es_client = AsyncElasticsearch(es_url, request_timeout=300)
    actions_generated = generate_actions(async_es_client, *args)
    await helpers.async_bulk(async_es_client, actions_generated)
    await async_es_client.close()




def extract_sources(response: ObjectApiResponse):
    # if 'hits' not in response:
    #     print(response)

    num_hits = response['hits']['total']['value']

    hits = response['hits']['hits']
    sources = [hit["_source"] for hit in hits]
    return sources, num_hits



def extract_hits_from_response(response: ObjectApiResponse):
    if 'hits' not in response:
        raise Exception(f'invalid response: {response}')

    return response['hits']['hits']


def get_query_payload(node_id: str, position: str, search_after: list | None):
    payload = {
        "size": 10000,
        "query": {
            "term": {
                f'{position}.keyword': node_id,
            }
        },
        "sort": [{"id": "asc"}]
    }

    if search_after is not None:
        payload["search_after"] = search_after

    return payload


def process_hits(node_id: str, position: str, hits: list | None, results: list):
    # initial query
    if hits is None:
        return get_query_payload(node_id, position, None)

    num_of_hits = len(hits)

    # update results if needed
    if num_of_hits > 0:
        sources = [hit["_source"] for hit in hits]
        results.extend(sources)

    # decide if this is last page
    if num_of_hits < 10000:
        return None


    # return body for next query based on pagination
    search_after = hits[-1]["sort"]
    return get_query_payload(node_id, position, search_after)


def clean_print(text: str):
    sys.stdout.write('\033[2K\r')  # Clear line and move cursor to beginning
    sys.stdout.flush()
    print(text, flush=True)


async def get_edges(es_client: AsyncElasticsearch, node_id: str):
    out_edges = []
    in_edges = []


    query_targets = [
        {
            "position": 'subject',
            "results": out_edges,
            "hits": None,
        },
        {
            "position": 'object',
            "results": in_edges,
            "hits": None,
        }
    ]

    while True:
        query_body = []

        next_query_targets = []

        for target in query_targets:
            next_query_body = process_hits(node_id, **target)


            if next_query_body is not None:
                # meta field
                query_body.append({})
                # actual query
                query_body.append(next_query_body)
                next_query_targets.append(target)


        if not query_body:
            break

        responses = await es_client.msearch(index=EDGE_INDEX, body=query_body)

        for index, target in enumerate(next_query_targets):
            response = responses["responses"][index]
            target['hits'] = extract_hits_from_response(response)


        query_targets = next_query_targets

    # clean_print(f'{node_id} in: {len(in_edges)} out: {len(out_edges)}')
    return out_edges, in_edges

async def process_single_node(es_client: AsyncElasticsearch, node_id: str):
    try:
        out_edges, in_edges = await get_edges(es_client, node_id)

    except Exception as e:
        print(e)
        return None, 0

    total_edges = len(in_edges) + len(out_edges)

    payload = {
        "_op_type": "update",
        "_index": ADJ_INDEX,
        "_id": node_id,
        "doc": {
            "out_edges": out_edges,
            "in_edges": in_edges,
        }
    }

    return payload, total_edges



if __name__ == "__main__":
    main()