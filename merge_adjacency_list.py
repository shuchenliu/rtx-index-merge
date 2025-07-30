import json
import os

from elastic_transport import ObjectApiResponse
from elasticsearch import Elasticsearch

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

def populate_by_node(es_client: Elasticsearch):
    target_file = './nodes_10k.jsonl'
    line_count = 0

    with open(target_file,"r") as f:
        for line in f:
            data = json.loads(line)
            node_id = data["id"]
            total_edges = process_single_node(es_client, node_id)
            line_count += 1
            print(f"Total lines processed: {line_count} with {total_edges} edges", end='\r')





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


    es_client.update(index=ADJ_INDEX, id=node_id, body={
        "doc": {
            "out_edges": out_edges,
            "in_edges": in_edges,
        }
    })

    return total_edges



if __name__ == "__main__":
    main()