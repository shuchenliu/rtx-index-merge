import json
from typing import Optional


def load_edges(target_file:str, start: int, end:Optional[int]) -> list:
    """
    Loads edges from file given start and ending byte locations.

    :param target_file: str, location of edges json file
    :param start: int, byte location for start of block
    :param end: int, byte location for end of block
    :return: a list loaded edges
    """
    with open(target_file, "rb") as f:
        f.seek(start)
        if end is None:
            block = f.read()
        else:
            block = f.read(end - start)

        lines = block.splitlines()
        loaded = list(map(json.loads, lines))

        return loaded