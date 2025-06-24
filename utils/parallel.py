from typing import Optional

from dask import delayed
from dask.distributed import Client, get_worker, LocalCluster, as_completed
from elasticsearch import Elasticsearch

from utils.constants import N_WORKERS, THREADS_PER_WORKER
from utils.edges import process_edges
from utils.writes import write_to_temp


@delayed
def delayed_task(es_url: str, target_file: str, index: int, start: int, end: Optional[int]):
    worker = get_worker()
    if not hasattr(worker, "es_client"):
        worker.es_client = Elasticsearch(es_url)

    updated_edges = process_edges(worker.es_client, target_file, start,                                  end)
    write_to_temp(index, updated_edges)
    n_edges_processed = len(updated_edges)

    del updated_edges

    return n_edges_processed

def distribute_tasks(*, es_url: str, target_file:str, offsets: list[int]):
    print(f"starting {N_WORKERS} workers with {THREADS_PER_WORKER}-thread each")

    cluster = LocalCluster(n_workers=N_WORKERS, threads_per_worker=THREADS_PER_WORKER)
    client = Client(cluster)

    tasks = []

    for index, start in enumerate(offsets):
        tasks.append(delayed_task(es_url, target_file, index, start, offsets[index + 1] if index + 1 < len(offsets) else None))

    futures = client.compute(tasks)

    total_lines_processed = 0
    for future in as_completed(futures):
        lines_processed = future.result()
        total_lines_processed += lines_processed
        print(f"Total lines processed: {total_lines_processed}", end='\r', flush=True)

    client.close()

    # client.gather(futures)
