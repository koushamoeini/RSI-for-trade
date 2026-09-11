from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


def _csv(value: str) -> tuple[str, ...]:
    return tuple(item.strip().upper() for item in value.split(",") if item.strip())


@dataclass(frozen=True)
class Settings:
    telegram_bot_token: str
    telegram_chat_id: str
    binance_base_url: str
    quote_asset: str
    symbols: tuple[str, ...]
    exclude_symbols: tuple[str, ...]
    interval: str
    rsi_period: int
    lower_threshold: float
    upper_threshold: float
    scan_every_seconds: int
    alert_cooldown_minutes: int
    max_concurrency: int
    request_timeout_seconds: float
    admin_chat_ids: tuple[str, ...]
    state_file: str

    @classmethod
    def from_env(cls) -> "Settings":
        load_dotenv()
        settings = cls(
            telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
            telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", "").strip(),
            binance_base_url=os.getenv(
                "BINANCE_BASE_URL", "https://data-api.binance.vision"
            ).rstrip("/"),
            quote_asset=os.getenv("QUOTE_ASSET", "USDT").strip().upper(),
            symbols=_csv(os.getenv("SYMBOLS", "ALL")),
            exclude_symbols=_csv(os.getenv("EXCLUDE_SYMBOLS", "")),
            interval=os.getenv("CANDLE_INTERVAL", "1h").strip(),
            rsi_period=int(os.getenv("RSI_PERIOD", "14")),
            lower_threshold=float(os.getenv("RSI_LOWER", "30")),
            upper_threshold=float(os.getenv("RSI_UPPER", "90")),
            scan_every_seconds=int(os.getenv("SCAN_EVERY_SECONDS", "300")),
            alert_cooldown_minutes=int(os.getenv("ALERT_COOLDOWN_MINUTES", "240")),
            max_concurrency=int(os.getenv("MAX_CONCURRENCY", "10")),
            request_timeout_seconds=float(os.getenv("REQUEST_TIMEOUT_SECONDS", "15")),
            admin_chat_ids=tuple(
                item.strip()
                for item in os.getenv("TELEGRAM_ADMIN_CHAT_IDS", "").split(",")
                if item.strip()
            ),
            state_file=os.getenv("STATE_FILE", "/app/data/settings.json").strip(),
        )
        settings.validate()
        return settings

    def validate(self) -> None:
        if not self.telegram_bot_token or not self.telegram_chat_id:
            raise ValueError("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are required")
        if self.rsi_period < 2:
            raise ValueError("RSI_PERIOD must be at least 2")
        if not 0 <= self.lower_threshold < self.upper_threshold <= 100:
            raise ValueError("RSI thresholds must satisfy 0 <= lower < upper <= 100")
        if self.scan_every_seconds < 10:
            raise ValueError("SCAN_EVERY_SECONDS must be at least 10")
        if self.max_concurrency < 1:
            raise ValueError("MAX_CONCURRENCY must be at least 1")

    @property
    def intervals(self) -> tuple[str, ...]:
        """Configured Binance candle intervals (comma-separated in the env file)."""
        values = tuple(item.strip() for item in self.interval.split(",") if item.strip())
        if not values:
            raise ValueError("CANDLE_INTERVAL must contain at least one timeframe")
        return values
