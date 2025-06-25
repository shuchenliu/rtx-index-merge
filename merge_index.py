import argparse
import os
import shutil
import subprocess

from dotenv import load_dotenv

from utils.benchmark import timeit
from utils.constants import TEMP_DIR, BATCH_SIZE
from utils.edges import refresh_es_index
from utils.make_offsets import get_offsets
from utils.parallel import distribute_tasks
from utils.writes import stitch_temps


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

    # assert index name is set
    INDEX_NAME = os.getenv("INDEX_NAME")
    assert INDEX_NAME is not None

    # overwrite edge file if specified
    parser = argparse.ArgumentParser()
    parser.add_argument("filepath", nargs="?", default=EDGE_FILE, help="Path to the input file")
    args = parser.parse_args()

    edge_file_path = args.filepath

    print(edge_file_path)

    os.makedirs(TEMP_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print ("Indexing offsets")
    offsets = get_offsets(edge_file_path, BATCH_SIZE)
    print("Offsets indexed:", len(offsets), "start locations")



    # make sure we have a clean slate
    refresh_es_index(ES_URL)

    return


    # set worker number as needed
    if len(offsets) < 10:
        os.environ["N_WORKERS"] = str(len(offsets))

    '''
    Consecutive run
    '''
    # es_client = Elasticsearch(ES_URL)
    # with timeit('consecutive tasks'):
    #     for start_index, start in enumerate(offsets):
    #         updated_edges = process_edges(es_client, edge_file_path, start, offsets[start_index + 1] if start_index + 1 < len(offsets) else None)
    #         write_to_temp(start_index, updated_edges)
    #
    #     subprocess.run(["./merge_temps.sh"], check=True)


    '''
    Distributed/parallel run
    '''
    with timeit('distributed tasks'):
        distribute_tasks(es_url=ES_URL, target_file=edge_file_path, offsets=offsets)
        # write final output file
        # stitch_temps(OUTPUT_DIR)
        # subprocess.run(["./merge_temps.sh", OUTPUT_DIR], check=True)

    # remove temp files
    shutil.rmtree('./temp_output')




if __name__ == "__main__":
    main()