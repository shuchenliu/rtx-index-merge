import json
import math
import multiprocessing
import os
from time import sleep

from elastic_transport import ObjectApiResponse
from elasticsearch import Elasticsearch, helpers

from utils.benchmark import timeit
from utils.env import check_is_prod, get_es_url
from utils.es import created_adjacency_list_index
from utils.constants import NODE_INDEX, EDGE_INDEX, ADJ_INDEX


def main():
    is_prod = check_is_prod()
    es_url = get_es_url()
    # migrate = created_adjacency_list_index(es_url)
    #
    # # migrate data from nodes to adj_list as a base
    # migrate()


    es_client = Elasticsearch(es_url, request_timeout=300)

    total_workers = 10

    progress_array = multiprocessing.Array('i', [0] * total_workers)

    # limit = 10000
    limit = 1500

    node_id_file = './nodes_id.json'
    node_ids = get_node_ids(node_id_file, limit)

    total_nodes = len(node_ids)
    nodes_per_worker = math.ceil(total_nodes / total_workers)

    # Start monitor
    monitor_proc = multiprocessing.Process(target=monitor_progress, args=(progress_array, total_nodes))
    monitor_proc.start()

    with timeit(f'process {limit} nodes'):
        # populate_by_node(es_client)

        # Spawn workers
        workers = []
        for i in range(total_workers):
            start = i * nodes_per_worker
            end = min(start + nodes_per_worker, total_nodes)
            chunk = node_ids[start:end]

            p = multiprocessing.Process(target=per_worker, args=(es_url, chunk, progress_array, i))
            p.start()
            workers.append(p)

        # Wait for all workers to finish
        for p in workers:
            p.join()

    monitor_proc.join()


def get_node_ids(target_file: str, limit=1500):
    with open(target_file, "rb") as f:
        full_ids = json.load(f)


    if limit is None:
        return full_ids


    # only use partial ids for testing
    node_ids = full_ids[:limit]
    del full_ids

    return node_ids


def monitor_progress(progress_array, total_count):
    while True:
        sleep(1)
        current_total = sum(progress_array)
        print(f"Progress: {current_total}/{total_count} nodes processed", end='\r', flush=True)
        if current_total >= total_count:
            print()  # newline after complete
            break

def _generate_actions(es_client: Elasticsearch, nodes_ids: list[str], progress_array: list, worker_id:int):
    processed = 0
    for node_id in nodes_ids:
        payload, total_edges = process_single_node(es_client, node_id)
        processed += 1
        progress_array[worker_id] += 1

        yield payload


def per_worker(es_url:str, nodes_ids: list[str], progress_array: list, worker_id:int):
    es_client = Elasticsearch(es_url)
    actions_generated = _generate_actions(es_client, nodes_ids, progress_array, worker_id)
    helpers.bulk(es_client, actions_generated, request_timeout=300)





def generate_actions(es_client: Elasticsearch, target_file: str):
    line_count = 0
    total_edges_processed = 0


    limit = 10000
    node_ids = get_node_ids(target_file, limit)

    assert len(node_ids) == limit

    for node_id in node_ids:
        payload, total_edges = process_single_node(es_client, node_id)
        line_count += 1
        total_edges_processed += total_edges

        if line_count % 1000 == 0:
            print(f"Total lines processed: {line_count} with {total_edges_processed / line_count} edges on average", end='\r', flush=True)

        yield payload


def populate_by_node(es_client: Elasticsearch):
    # target_file = './nodes_10k.jsonl'
    target_file = './nodes_id.json'

    helpers.bulk(es_client, generate_actions(es_client, target_file), chunk_size=1000, request_timeout=300)






def extract_sources(response: ObjectApiResponse):
    num_hits = response['hits']['total']['value']

    hits = response['hits']['hits']
    sources = [hit["_source"] for hit in hits]
    return sources, num_hits

def get_edges(es_client: Elasticsearch, node_id: str):
    queries = [
        ("subject.keyword", node_id),
        ("object.keyword", node_id),
    ]

    query_body = []

    for field, value in queries:
        query_body.append({})
        query_body.append({
            "size": 10000,
            "query": {
                "term": {
                    field: value
                }
            }
        })

    responses = es_client.msearch(index=EDGE_INDEX, body=query_body)

    out_edges, num_out_edges = extract_sources(responses["responses"][0])
    in_edges, num_in_edges = extract_sources(responses["responses"][1])

    if num_out_edges > 10000:
        print(f'{node_id} out edges out of limit: {num_out_edges}')

    if num_in_edges > 10000:
        print(f'{node_id} in edges out of limit: {num_in_edges}')

    return out_edges, in_edges


def process_single_node(es_client: Elasticsearch, node_id: str):
    out_edges, in_edges = get_edges(es_client, node_id)

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