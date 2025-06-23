import json
import os
import subprocess
from functools import reduce
from typing import Optional

from elasticsearch import Elasticsearch

from utils.edges import load_edges
from utils.make_offsets import get_offsets
from utils.nodes import get_nodes_details
from utils.writes import write_to_temp, stitch_temps

# 1. read in edges or query (scroll) end point
# 2. per 1000, call node endpoint to get more info
# 3. merge and save

# maybe use Dask


SERVER = "localhost"
PORT = 9200
ES_URL = "http://%s:%d" % (SERVER, PORT)


EDGE_FILE = "edges_sample.jsonl"


def process_edges(start: int, end: Optional[int]) -> list[str]:
    loaded = load_edges(EDGE_FILE, start, end)
    # todo use worker to get client
    client = Elasticsearch(ES_URL)


    # 0. get `subject` and `object`
    def ids_getter(id_set: set, edge: dict):
        if "subject" in edge:
            id_set.add(edge["subject"])
        if "object" in edge:
            id_set.add(edge["object"])

        return id_set

    ids = reduce(ids_getter, loaded, set()) # functional programming


    # 1. use es to get details
    details = get_nodes_details(client, list(ids))

    # 2. update edges and write back to file
    for index, edge in enumerate(loaded):
        if "subject" in edge:
            edge["subject"] = details[edge["subject"]]
        if "object" in edge:
            edge["object"] = details[edge["object"]]

        loaded[index] = json.dumps(edge)


    return loaded



def main():
    os.makedirs("temp_output", exist_ok=True)
    offsets = get_offsets(EDGE_FILE)

    for start_index, start in enumerate(offsets):
        updated_edges = process_edges(start, offsets[start_index + 1] if start_index + 1 < len(offsets) else None)
        write_to_temp(start_index, updated_edges)

    # write final output file
    # stitch_temps()
    subprocess.run(["./merge_temps.sh"], check=True)



if __name__ == "__main__":
    main()