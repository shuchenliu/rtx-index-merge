# How

## prerequisites
`Python 3.12` required. Please install dependencies with virtual env of choice.

## dev
`$ python merge_index.py <path to edges.jsonl>`

In dev mode, the script will try to interact with `Elasticsearch` instance at `http://localhost:9200`. This will create `merged_edges.jsonl` at `./output`. Since it generates local files only, it is safe to forward `su12` es instance locally or edit `.env.dev` files so the script interacts with `su12` directly.

## prod
`$ PROD=true python merge_index.py <path to edges.jsonl>`
The script will interact with `Elasticsearch` instance hosted at `su12`. ***CAUTION: This command WILL replace `rtx-kg2-edges-merged` index on su12. No local ouput will be generated***

# Misc
1. If not provided, the script will attempt to generate `offsets.json` file based on given edges to enable random access by multiprocessing workers. Therefore, it's recommended to start with smaller datasets (~100k lines).
2. If `offsets.json` already exists, the script will reuse it. This could be an issue if `offsets` do not match `edges` provided. It is recommended to delete `offsets.json` when using different `edges` inputs.
3. Prebuilt offsets for the full rtx-kg2 edges can be found at `su08:/opt/db_benchmarking/data/rtx_kg2/full_offsets.json` 
