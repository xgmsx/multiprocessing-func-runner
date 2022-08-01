import pytest
from unittest.mock import patch, Mock

from multiprocessing_func_runner import run_function


def test_run_function(square_fixture):
    assert run_function(square_fixture, range(4), pool_size=1) == [0, 1, 4, 9]


def test_run_function_empty_list(square_fixture):
    assert run_function(square_fixture, [], pool_size=1) == []


def test_run_function_no_exception():
    assert run_function(round, (0.333, 0.222, 0.111), pool_size=1, kwargs={'ndigits': 1}) == [0.3, 0.2, 0.1]


def test_run_function_exception():
    with pytest.raises(RuntimeError, match='TypeError'):
        run_function(round, (0.333, 0.222, 0.111), pool_size=1, kwargs={'ndigits': 'zero'})


def test_run_function_timeout_success(square_fixture):
    run_function(square_fixture, range(9), pool_size=1, timeout=1, kwargs={'delay': 0.1})


def test_run_function_timeout_error(square_fixture):
    with pytest.raises(TimeoutError):
        run_function(square_fixture, range(19), pool_size=1, timeout=1, kwargs={'delay': 0.1})


def test_run_function_arg_name():
    assert run_function(
        round, (1, 2, 3), pool_size=1, arg_name='ndigits', kwargs={'number': 0.11111}) == [0.1, 0.11, 0.111]


def test_run_function_keyboard_interrupt(square_fixture):
    with patch("multiprocessing_func_runner._utils.force_terminate_pool", Mock(return_value=None)):
        with pytest.raises(KeyboardInterrupt):
            run_function(square_fixture, [1, 2, 3], pool_size=2, kwargs={'with_ki': True})
