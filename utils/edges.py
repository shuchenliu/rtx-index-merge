import json
from functools import reduce
from typing import Optional

from elasticsearch import Elasticsearch
from utils.nodes import get_nodes_details


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


def process_edges(es_client: Elasticsearch, target_file:str, start: int, end: Optional[int]) -> list[str]:
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

        loaded[index] = json.dumps(edge)


    return loaded