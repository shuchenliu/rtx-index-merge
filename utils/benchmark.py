import time
from contextlib import contextmanager

@contextmanager
def timeit(name="Block"):
    start = time.time()
    yield
    end = time.time()
    print(f"{name} took {end - start:.2f} seconds")