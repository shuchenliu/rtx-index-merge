import os
import shutil
import subprocess

from dotenv import load_dotenv

from utils.benchmark import timeit
from utils.constants import TEMP_DIR
from utils.make_offsets import get_offsets
from utils.parallel import distribute_tasks

# 1. read in edges, indexing starting location for 10k batches
# 2. for each batch, extract sub and obj id and call ES for details
# 3. write updated edges to tem files
# 4. merge updated edges



def load_env():
    is_prod = os.getenv("PROD") == "true"
    if is_prod:
        load_dotenv('.env.prod')
        return

    load_dotenv('.env.dev')
    is_in_docker = os.getenv("IN_DOCKER") == "true"
    if is_in_docker:
        os.environ["SERVER"] = "host.docker.internal"


def main():

    # load envs
    load_env()
    SERVER = os.getenv("SERVER")
    PORT = os.getenv("PORT")
    EDGE_FILE = os.getenv("EDGE_FILE")
    OUTPUT_DIR = os.getenv("OUTPUT_DIR")

    ES_URL = "http://%s:%s" % (SERVER, PORT)


    os.makedirs(TEMP_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

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
        subprocess.run(["./merge_temps.sh", OUTPUT_DIR], check=True)

    # remove temp files
    shutil.rmtree('./temp_output')




if __name__ == "__main__":
    main()