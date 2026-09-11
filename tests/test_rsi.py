import pytest

from rsi_alert.rsi import wilder_rsi


def test_rsi_known_wilder_example():
    closes = [
        44.34, 44.09, 44.15, 43.61, 44.33, 44.83, 45.10, 45.42, 45.84,
        46.08, 45.89, 46.03, 45.61, 46.28, 46.28,
    ]
    assert wilder_rsi(closes, 14) == pytest.approx(70.4641, abs=0.0001)


def test_rsi_all_up_and_flat():
    assert wilder_rsi([float(i) for i in range(20)], 14) == 100.0
    assert wilder_rsi([10.0] * 20, 14) == 50.0


def test_rsi_needs_enough_data():
    with pytest.raises(ValueError):
        wilder_rsi([1.0, 2.0], 14)
