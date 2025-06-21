import glob

from utils.constants import TEMP_DIR


def write_to_temp(batch_id: int, edges: list[str]):
    """
    Write serialized edges to a temporary file.
    """
    with open(f"{TEMP_DIR}/{batch_id:05d}.tmp.jsonl", "w") as f:
        f.write("\n".join(edges) + '\n')

def stitch_temps():
    with open('merged_edges.jsonl', 'wb') as output:
        for filepath in sorted(glob.glob(f"{TEMP_DIR}/*.tmp.jsonl")):
            with open(filepath, "rb") as infile:
                output.write(infile.read())