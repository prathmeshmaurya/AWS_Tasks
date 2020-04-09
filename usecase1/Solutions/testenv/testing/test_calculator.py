
import pytest
from . import calculator

NUMBER_1 = 3.0
NUMBER_2 = 2.0


def test_add():
    value = calculator.add(NUMBER_1, NUMBER_2)
    assert value == 5.0


def test_subtract():
    value = calculator.subtract(NUMBER_1, NUMBER_2)
    assert value == 1.0


def test_subtract_negative():
    value = calculator.subtract(NUMBER_2, NUMBER_1)
    assert value == -1.0


def test_multiply():
    value = calculator.multiply(NUMBER_1, NUMBER_2)
    assert value == 6.0

