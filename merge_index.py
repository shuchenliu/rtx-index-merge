import os
import shutil
import subprocess

from elasticsearch import Elasticsearch

from utils.benchmark import timeit
from utils.edges import process_edges
from utils.make_offsets import get_offsets
from utils.parallel import distribute_tasks
from utils.writes import write_to_temp

# 1. read in edges or query (scroll) end point
# 2. per 1000, call node endpoint to get more info
# 3. merge and save

# maybe use Dask


SERVER = "localhost"
PORT = 9200
ES_URL = "http://%s:%d" % (SERVER, PORT)
EDGE_FILE = "edges_100k.jsonl"


def main():
    os.makedirs("temp_output", exist_ok=True)
    os.makedirs("output", exist_ok=True)

    print ("Indexing offsets")
    offsets = get_offsets(EDGE_FILE)
    print("Offsets indexed:", len(offsets) , "start locations")


    '''
    Consecutive run
    '''
    # es_client = Elasticsearch(ES_URL)
    # with timeit('consecutive tasks'):
    #     for start_index, start in enumerate(offsets):
    #         updated_edges = process_edges(es_client, EDGE_FILE, start, offsets[start_index + 1] if start_index + 1 < len(offsets) else None)
    #         write_to_temp(start_index, updated_edges)
    #
    #     subprocess.run(["./merge_temps.sh"], check=True)


    '''
    Distributed/parallel run
    '''
    with timeit('distributed tasks'):
        distribute_tasks(es_url=ES_URL, target_file=EDGE_FILE, offsets=offsets)
        # write final output file
        # stitch_temps()
        subprocess.run(["./merge_temps.sh"], check=True)

    # remove temp files
    shutil.rmtree('./temp_output')




if __name__ == "__main__":
    main()