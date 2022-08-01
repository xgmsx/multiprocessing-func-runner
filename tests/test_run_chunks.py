
from multiprocessing_func_runner import run_function_chunks, unpack_results


def test_run_function_chunks():
    assert run_function_chunks(
        sum, range(10), pool_size=1, chunk_size=3) == [3, 12, 21, 9]


def test_run_function_chunks_without_chunk_size(squares_list_fixture):
    assert run_function_chunks(
        squares_list_fixture, range(10), pool_size=1) == [[0, 1, 4], [9, 16, 25], [36, 49, 64], [81]]

    assert sorted(run_function_chunks(
        squares_list_fixture, range(10), pool_size=2)) == [[0, 1], [4, 9], [16, 25], [36, 49], [64, 81]]


def test_run_function_chunks_unpack_chunks(squares_list_fixture):
    results = run_function_chunks(
        squares_list_fixture, range(10), pool_size=1, chunk_size=3, kwargs={'delay': 0.1})

    assert results == [[0, 1, 4], [9, 16, 25], [36, 49, 64], [81]]

    assert unpack_results(results) == [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]
