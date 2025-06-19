def make_offsets(target_file:str, batch_size=10000) -> list[int]:
    """
    Make byte offsets for given batch size.
    e.g. if the batch size is 10k, it will give byte location for line 0, line 10k ...
    :param
        batch_size: number, default 10000
        target_file: string, reference filename
    :return: a list of byte offsets, denoting starting location of each batch
    """
    offsets = []
    offset = 0
    line_count = 0

    with open(target_file, "rb") as f:
        for line in f:
            if line_count % batch_size == 0:
                offsets.append(offset)

            offset += len(line)
            line_count += 1

    return offsets