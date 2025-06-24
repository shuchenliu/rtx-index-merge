import os

NODE_INDEX="rtx_kg2_nodes"
TEMP_DIR="temp_output"
OFFSET_OUTPUT="offsets.json"

N_WORKERS = int(os.getenv("N_WORKERS", 10))
THREADS_PER_WORKER=1
