from utils.make_offsets import make_offsets

# 1. read in edges or query (scroll) end point
# 2. per 1000, call node endpoint to get more info
# 3. merge and save

# maybe use Dask


SERVER = "localhost"
PORT = 9200
ES_URL = "http://%s:%d" % (SERVER, PORT)


EDGE_FILE = "edges_sample.jsonl"

offsets = make_offsets(EDGE_FILE)
print(offsets)