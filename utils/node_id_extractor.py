import json

from benchmark import timeit


def main():
    name = 'nodes'

    with timeit(f"extract {name} ids"):
        ids = list(map(extract_id, get_line(name)))
        write_locally(name, ids)
        ids_loaded = read_in_ids(name)
        assert len(ids_loaded) == len(ids)
        print(f'Loaded {len(ids_loaded)} ids')


def read_in_ids(name):
    with open(f'../{name}_ids.json', 'rb') as f:
        return json.load(f)


def write_locally(name: str, ids: list[str]):
    with open(f'../{name}_ids.json', 'w') as f:
        json.dump(ids, f)

def extract_id(line: str) -> str:
    data = json.loads(line)
    return data["id"]

def get_line(name: str):
    with open(f'../{name}.jsonl', 'r') as f:
        for line in f:
            yield line



if __name__ == "__main__":
    main()