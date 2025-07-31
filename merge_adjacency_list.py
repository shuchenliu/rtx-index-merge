import json
import os

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

    with timeit('process 10k nodes'):
        populate_by_node(es_client)

def get_node_ids(target_file: str, limit=1500):
    with open(target_file, "rb") as f:
        full_ids = json.load(f)

    # only use partial ids for testing
    node_ids = full_ids[:limit]
    del full_ids

    return node_ids


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