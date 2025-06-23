import json
import os
import subprocess
from functools import reduce
from typing import Optional

from elasticsearch import Elasticsearch

from utils.edges import load_edges, process_edges
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




def main():
    os.makedirs("temp_output", exist_ok=True)
    offsets = get_offsets(EDGE_FILE)
    es_client = Elasticsearch(ES_URL)

    for start_index, start in enumerate(offsets):
        updated_edges = process_edges(es_client, EDGE_FILE, start, offsets[start_index + 1] if start_index + 1 < len(offsets) else None)
        write_to_temp(start_index, updated_edges)

    # write final output file
    # stitch_temps()
    subprocess.run(["./merge_temps.sh"], check=True)



if __name__ == "__main__":
    main()