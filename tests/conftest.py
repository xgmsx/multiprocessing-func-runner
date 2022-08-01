import pytest
import time


def square(n: int, delay=None, with_ki: bool = False):
    if delay:
        time.sleep(delay)
    if with_ki:
        raise KeyboardInterrupt
    return n * n


def squares(lst: list, delay=None):
    results = list()
    for n in lst:
        if delay:
            time.sleep(delay)
        results.append(n * n)
    return results


@pytest.fixture(scope="module")
def square_fixture():
    return square


@pytest.fixture(scope="module")
def squares_list_fixture():
    return squares
