import glob

from utils.constants import TEMP_DIR


def write_to_temp(batch_id: int, edges: list[str]):
    """
    Write serialized edges to a temporary file.
    """
    with open(f"{TEMP_DIR}/{batch_id:05d}.tmp.jsonl", "w") as f:
        f.write("\n".join(edges) + '\n')

def stitch_temps(output_dir: str):
    files = sorted(glob.glob(f"{TEMP_DIR}/*.tmp.jsonl"))
    total = len(files)

    with open(f'{output_dir}/merged_edges.jsonl', 'wb') as output:
        for i, filepath in enumerate(files, 1):
            with open(filepath, "rb") as infile:
                output.write(infile.read())
            print(f"\rStitched {i}/{total} files", end='', flush=True)

    print("\nDone.")