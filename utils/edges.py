import json
import os
from functools import reduce
from typing import Optional

from elasticsearch import Elasticsearch

from utils.constants import EDGE_INDEX
from utils.es import get_es_docs_using_ids, insert_docs_to_index
from utils.nodes import get_nodes_details


def load_edge_ids(target_file:str, start: int, end:Optional[int]) -> list[str]:
    """
    Loads edges from file given start and ending byte locations.

    :param target_file: str, location of edges json file
    :param start: int, byte location for start of block
    :param end: int, byte location for end of block
    :return: a list of ids of loaded edges
    """
    def id_loader(line: bytes):
        data = json.loads(line)
        return data["id"]

    with open(target_file, "rb") as f:
        f.seek(start)
        if end is None:
            block = f.read()
        else:
            block = f.read(end - start)

        lines = block.splitlines()
        # loaded = list(map(json.loads, lines))

        loaded_ids = list(set(map(id_loader, lines)))


        return loaded_ids

def load_edges(es_client: Elasticsearch, target_file: str, start: int, end: Optional[int]) -> list:
    loaded_edge_ids = load_edge_ids(target_file, start, end)
    return get_es_docs_using_ids(es_client, EDGE_INDEX, loaded_edge_ids)



def process_edges(es_client: Elasticsearch, target_file:str, start: int, end: Optional[int]) -> int:
    loaded = load_edges(es_client, target_file, start, end)
    # 0. get `subject` and `object`
    def ids_getter(id_set: set, edge: dict):
        if "subject" in edge:
            id_set.add(edge["subject"])
        if "object" in edge:
            id_set.add(edge["object"])

        return id_set

    node_ids = reduce(ids_getter, loaded, set()) # functional programming


    # 1. use es to get details
    node_details = get_nodes_details(es_client, list(node_ids))

    # 2. update edges and write back to file
    for index, edge in enumerate(loaded):
        if "subject" in edge:
            edge["subject"] = node_details[edge["subject"]]
        if "object" in edge:
            edge["object"] = node_details[edge["object"]]

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
