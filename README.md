# Multiprocessing function runner

A simple package to quickly run any function in multiprocessing mode

# Usage

```python
from multiprocessing_func_runner import run_function

def square(a):
    return a * a

results = run_function(square, iterable=[a for a in range(10)], pool_size=8)

# [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]
```

```python
from multiprocessing_func_runner import run_function

def multiply(a, b):
    return a * b

results = run_function(
    func=multiply, iterable=[a for a in range(10)], pool_size=8, 
    arg_name='b', kwargs={'a': 10})

# [0, 10, 20, 30, 40, 50, 60, 70, 80, 90]
```

# Testing

```shell
$ pytest -v
$ pytest --cov-report term --cov-report html --cov=. 
```
