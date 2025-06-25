import json
import os
from functools import reduce
from typing import Optional

from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import BulkIndexError

from utils.nodes import get_nodes_details


def refresh_es_index(es_url: str):
    es_client = Elasticsearch(es_url)
    INDEX_NAME = os.environ.get("INDEX_NAME")

    

    node_mapping = es_client.indices.get_mapping(index="rtx_kg2_nodes")
    node_props = node_mapping["rtx_kg2_nodes"]["mappings"]["properties"]

    edge_mapping = es_client.indices.get_mapping(index="rtx_kg2_edges")
    edge_props = edge_mapping["rtx_kg2_edges"]["mappings"]["properties"]

    # edge_props["subject"] = {"properties": node_props}
    # edge_props["object"] = {"properties": node_props}

    subject_object_mapping = {
        "properties": {
            "id": {
                "type": "keyword"
            }
        },
        "type": 'object',
        "dynamic": False,
    }

    edge_props["subject"] = subject_object_mapping
    edge_props["object"] = subject_object_mapping

    index_settings_and_mappings = {
        "mappings": {
            "properties": edge_props
        },
        "settings": {
            "number_of_shards": 5,
            "number_of_replicas": 0,
            "codec": "best_compression"
        }
    }

    # refresh index
    if es_client.indices.exists(index=INDEX_NAME):
        es_client.indices.delete(index=INDEX_NAME)

    es_client.indices.create(index=INDEX_NAME, body=index_settings_and_mappings)



def load_edges(target_file:str, start: int, end:Optional[int]) -> list:
    """
    Loads edges from file given start and ending byte locations.

    :param target_file: str, location of edges json file
    :param start: int, byte location for start of block
    :param end: int, byte location for end of block
    :return: a list loaded edges
    """
    with open(target_file, "rb") as f:
        f.seek(start)
        if end is None:
            block = f.read()
        else:
            block = f.read(end - start)

        lines = block.splitlines()
        loaded = list(map(json.loads, lines))

        return loaded


def process_edges(es_client: Elasticsearch, target_file:str, start: int, end: Optional[int]) -> int:
    loaded = load_edges(target_file, start, end)
    # 0. get `subject` and `object`
    def ids_getter(id_set: set, edge: dict):
        if "subject" in edge:
            id_set.add(edge["subject"])
        if "object" in edge:
            id_set.add(edge["object"])

        return id_set

    ids = reduce(ids_getter, loaded, set()) # functional programming


    # 1. use es to get details
    details = get_nodes_details(es_client, list(ids))

    # 2. update edges and write back to file
    for index, edge in enumerate(loaded):
        if "subject" in edge:
            edge["subject"] = details[edge["subject"]]
        if "object" in edge:
            edge["object"] = details[edge["object"]]

        # loaded[index] = json.dumps(edge)

        # prepare for insertions
        loaded[index] = {
            "_index": os.getenv("INDEX_NAME"),
            "_id": edge['id'],
            "_source": edge
        }

    insert_docs_to_index(es_client, loaded)
    num_processed = len(loaded)

    del loaded

    return num_processed


def insert_docs_to_index(es_client: Elasticsearch, operations: list):
    try:
        helpers.bulk(es_client, operations, chunk_size=5000, request_timeout=240)
        # helpers.bulk(es_client, operations, chunk_size=5000)
    except BulkIndexError as e:
        for i, error in enumerate(e.errors[:1]):  # Only show first
            doc_id = error["index"].get("_id", "N/A")
            reason = error["index"]["error"].get("reason", "Unknown")
            print(f"[{i}] ID={doc_id} → {reason}")