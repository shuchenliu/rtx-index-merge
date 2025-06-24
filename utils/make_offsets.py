import json
import os

from utils.constants import OFFSET_OUTPUT

def write_offsets(offsets):
    with open(OFFSET_OUTPUT,"w") as f:
        json.dump(offsets,f)

def make_offsets(target_file:str, batch_size:int):
    offsets = []
    offset = 0
    line_count = 0

    with open(target_file, "rb") as f:
        for line in f:
            if line_count % batch_size == 0:
                offsets.append(offset)

            offset += len(line)
            line_count += 1

    print("total lines", line_count)

    return offsets

def get_offsets(target_file:str, batch_size=10000, force=False) -> list[int]:
    """
    Make byte offsets for given batch size.
    e.g. if the batch size is 10k, it will give byte location for line 0, line 10k ...
    :param
        batch_size: number, default 10000
        target_file: string, reference filename
    :return: a list of byte offsets, denoting starting location of each batch
    """


    if force or not os.path.exists(OFFSET_OUTPUT):
        offsets = make_offsets(target_file, batch_size)
        write_offsets(offsets)

        return offsets

    # if offset file already exists, use that unless forced not to
    with open(OFFSET_OUTPUT, "rb") as offset_file:
        loaded_offsets = json.load(offset_file)
        return loaded_offsets

