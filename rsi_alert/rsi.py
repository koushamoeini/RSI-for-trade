from __future__ import annotations


def wilder_rsi(closes: list[float], period: int = 14) -> float:
    """Return Wilder's RSI for the last close in the series."""
    if period < 2:
        raise ValueError("period must be at least 2")
    if len(closes) < period + 1:
        raise ValueError(f"need at least {period + 1} closes")

    changes = [current - previous for previous, current in zip(closes, closes[1:])]
    gains = [max(change, 0.0) for change in changes]
    losses = [max(-change, 0.0) for change in changes]
    average_gain = sum(gains[:period]) / period
    average_loss = sum(losses[:period]) / period

    for gain, loss in zip(gains[period:], losses[period:]):
        average_gain = (average_gain * (period - 1) + gain) / period
        average_loss = (average_loss * (period - 1) + loss) / period

    if average_loss == 0:
        return 100.0 if average_gain > 0 else 50.0
    relative_strength = average_gain / average_loss
    return 100.0 - (100.0 / (1.0 + relative_strength))

