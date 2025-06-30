# How

## prerequisites
`Python 3.12` required. Please install dependencies with virtual env of choice.

## dev
`$ python merge_index.py <path to edges.jsonl>`

In dev mode, the script will try to interact with `Elasticsearch` instance at `http://localhost:9200`. This will create `merged_edges.jsonl` at `./output`. Since it ouputs only local files, it is safe to forward `su12` es instance locally or edit `.env.dev` files so the script interacts with `su12` directly
