from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import time
from dataclasses import dataclass, field
from pathlib import Path

import httpx

from .config import Settings
from .rsi import wilder_rsi

LOGGER = logging.getLogger("rsi-alert")


@dataclass
class AlertState:
    zone: str = "normal"
    last_alert_at: float = 0.0


@dataclass
class AlertPreferences:
    high_enabled: bool = True
    high_threshold: float = 87.0
    low_enabled: bool = False
    low_threshold: float = 30.0
    intervals: list[str] = field(default_factory=lambda: ["30m", "1d"])
    subscribers: list[str] = field(default_factory=list)


class RSIAlertService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.states: dict[str, AlertState] = {}
        self.preferences = self.load_preferences()
        self.telegram_offset: int | None = None
        self.stop_event = asyncio.Event()
        self.semaphore = asyncio.Semaphore(settings.max_concurrency)
        self.telegram_lock = asyncio.Lock()
        self.last_telegram_send = 0.0
        self.client = httpx.AsyncClient(
            timeout=settings.request_timeout_seconds,
            headers={"User-Agent": "rsi-telegram-alert/1.0"},
        )

    def load_preferences(self) -> AlertPreferences:
        path = Path(self.settings.state_file)
        default_subscribers = [
            item.strip()
            for item in self.settings.telegram_chat_id.split(",")
            if item.strip()
        ]
        try:
            values = json.loads(path.read_text(encoding="utf-8"))
            return AlertPreferences(
                high_enabled=bool(values.get("high_enabled", True)),
                high_threshold=float(values.get("high_threshold", 87)),
                low_enabled=bool(values.get("low_enabled", False)),
                low_threshold=float(values.get("low_threshold", 30)),
                intervals=list(values.get("intervals", ["30m", "1d"])),
                subscribers=[
                    str(item)
                    for item in values.get("subscribers", default_subscribers)
                ],
            )
        except FileNotFoundError:
            return AlertPreferences(subscribers=default_subscribers)
        except Exception as exc:
            LOGGER.warning("Could not load saved settings; using defaults: %s", exc)
            return AlertPreferences()

    def save_preferences(self) -> None:
        path = Path(self.settings.state_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".tmp")
        temporary.write_text(
            json.dumps(self.preferences.__dict__, indent=2), encoding="utf-8"
        )
        os.replace(temporary, path)

    def settings_text(self) -> str:
        high = "روشن ✅" if self.preferences.high_enabled else "خاموش ❌"
        low = "روشن ✅" if self.preferences.low_enabled else "خاموش ❌"
        return (
            "⚙️ تنظیمات هشدار RSI\n\n"
            f"📈 High: {high} — بیشتر از {self.preferences.high_threshold:g}\n"
            f"📉 Low: {low} — کمتر از {self.preferences.low_threshold:g}\n"
            f"🏦 بازار: Toobit {'Futures (USDT-M)' if self.settings.market_type == 'futures' else 'Spot'}\n"
            f"⏱ تایم‌فریم‌ها: {', '.join(self.preferences.intervals)}\n\n"
            "برای تغییر تنظیمات، دکمه‌های زیر را بزنید."
        )

    def settings_keyboard(self) -> dict[str, object]:
        p = self.preferences
        high_toggle = "📈 High: روشن ✅" if p.high_enabled else "📈 High: خاموش ❌"
        low_toggle = "📉 Low: روشن ✅" if p.low_enabled else "📉 Low: خاموش ❌"
        timeframe_buttons = [
            {
                "text": ("✅ " if value in p.intervals else "⬜ ") + value,
                "callback_data": f"tf:{value}",
            }
            for value in ("15m", "30m", "1h", "4h", "1d")
        ]
        return {
            "inline_keyboard": [
                [{"text": high_toggle, "callback_data": "toggle:high"}],
                [
                    {"text": "−5", "callback_data": "adjust:high:-5"},
                    {"text": "−1", "callback_data": "adjust:high:-1"},
                    {"text": "+1", "callback_data": "adjust:high:1"},
                    {"text": "+5", "callback_data": "adjust:high:5"},
                ],
                [{"text": low_toggle, "callback_data": "toggle:low"}],
                [
                    {"text": "−5", "callback_data": "adjust:low:-5"},
                    {"text": "−1", "callback_data": "adjust:low:-1"},
                    {"text": "+1", "callback_data": "adjust:low:1"},
                    {"text": "+5", "callback_data": "adjust:low:5"},
                ],
                timeframe_buttons[:3],
                timeframe_buttons[3:],
                [{"text": "🔄 بروزرسانی", "callback_data": "refresh"}],
            ]
        }

    async def close(self) -> None:
        await self.client.aclose()

    async def telegram(self, text: str) -> None:
        url = f"https://api.telegram.org/bot{self.settings.telegram_bot_token}/sendMessage"
        chat_ids = list(self.preferences.subscribers)
        for chat_id in chat_ids:
            # Telegram recommends no more than roughly one message/second per chat.
            async with self.telegram_lock:
                delay = 1.05 - (time.monotonic() - self.last_telegram_send)
                if delay > 0:
                    await asyncio.sleep(delay)
                for attempt in range(2):
                    response = await self.client.post(
                        url,
                        json={"chat_id": chat_id, "text": text},
                    )
                    if response.status_code != 429 or attempt == 1:
                        response.raise_for_status()
                        data = response.json()
                        if not data.get("ok"):
                            raise RuntimeError(
                                f"Telegram rejected message: {data.get('description')}"
                            )
                        self.last_telegram_send = time.monotonic()
                        break
                    retry_after = response.json().get("parameters", {}).get(
                        "retry_after", 2
                    )
                    await asyncio.sleep(float(retry_after) + 0.1)

    async def symbols(self) -> list[str]:
        if self.settings.symbols != ("ALL",):
            selected: list[str] = []
            for symbol in self.settings.symbols:
                if symbol in self.settings.exclude_symbols:
                    continue
                if self.settings.market_type == "futures":
                    if "-SWAP-" not in symbol:
                        symbol = f"{symbol.removesuffix(self.settings.quote_asset)}-SWAP-{self.settings.quote_asset}"
                elif not symbol.endswith(self.settings.quote_asset):
                    symbol += self.settings.quote_asset
                selected.append(symbol)
            return selected

        response = await self.client.get(
            f"{self.settings.market_base_url}/api/v1/exchangeInfo"
        )
        response.raise_for_status()
        excluded = set(self.settings.exclude_symbols)
        if self.settings.market_type == "futures":
            return sorted(
                item["symbol"]
                for item in response.json().get("contracts", [])
                if item["status"] == "TRADING"
                and item.get("quoteAsset") == self.settings.quote_asset
                and not item.get("inverse", False)
                and item["symbol"] not in excluded
                and item.get("underlying", "") not in excluded
            )
        return sorted(
            item["symbol"]
            for item in response.json()["symbols"]
            if item["status"] == "TRADING"
            and item["quoteAsset"] == self.settings.quote_asset
            and item.get("isSpotTradingAllowed", True)
            and item["symbol"] not in excluded
        )

    def display_symbol(self, symbol: str) -> str:
        if "-SWAP-" in symbol:
            return symbol.replace("-SWAP-", "/") + " Perpetual"
        if symbol.endswith(self.settings.quote_asset):
            base = symbol[: -len(self.settings.quote_asset)]
            return f"{base}/{self.settings.quote_asset}"
        return symbol

    async def get_rsi(self, symbol: str, interval: str | None = None) -> tuple[float, float]:
        # Fetch extra history so Wilder smoothing is not based on only 14 candles.
        limit = max(self.settings.rsi_period * 8, self.settings.rsi_period + 2)
        candle_interval = interval or self.settings.intervals[0]
        async with self.semaphore:
            response = await self.client.get(
                f"{self.settings.market_base_url}/quote/v1/klines",
                params={"symbol": symbol, "interval": candle_interval, "limit": limit},
            )
            response.raise_for_status()
        candles = response.json()
        if len(candles) < self.settings.rsi_period + 2:
            raise ValueError("not enough candle history")
        # The final item is the currently open candle; deliberately ignore it.
        closes = [float(candle[4]) for candle in candles[:-1]]
        return wilder_rsi(closes, self.settings.rsi_period), closes[-1]

    def zone_for(self, rsi: float) -> str:
        if self.preferences.low_enabled and rsi < self.preferences.low_threshold:
            return "low"
        if self.preferences.high_enabled and rsi > self.preferences.high_threshold:
            return "high"
        return "normal"

    async def handle_command(self, chat_id: str, text: str) -> None:
        parts = text.strip().lower().split()
        command = parts[0].split("@", 1)[0] if parts else ""
        if command == "/start":
            if chat_id not in self.preferences.subscribers:
                self.preferences.subscribers.append(chat_id)
                self.save_preferences()
            if chat_id in self.settings.admin_chat_ids:
                await self.telegram_to(
                    chat_id,
                    "سلام 🌷\nهشدارها برای شما فعال شد.\n\n" + self.settings_text(),
                    self.settings_keyboard(),
                )
            else:
                await self.telegram_to(
                    chat_id,
                    "سلام 👋\nهشدارهای RSI برای شما فعال شد 🔔\n\n"
                    "تنظیمات توسط مدیر ربات انجام می‌شود.",
                    self.subscription_keyboard(True),
                )
            return
        if command in ("/stop", "/unsubscribe"):
            if chat_id in self.preferences.subscribers:
                self.preferences.subscribers.remove(chat_id)
                self.save_preferences()
            await self.telegram_to(
                chat_id, "دریافت هشدارها متوقف شد 🔕", self.subscription_keyboard(False)
            )
            return
        if command == "/help" and chat_id not in self.settings.admin_chat_ids:
            subscribed = chat_id in self.preferences.subscribers
            await self.telegram_to(
                chat_id,
                "با دکمه زیر می‌توانید دریافت هشدارها را فعال یا متوقف کنید.",
                self.subscription_keyboard(subscribed),
            )
            return
        if chat_id not in self.settings.admin_chat_ids:
            await self.telegram_to(
                chat_id, "⛔ فقط مدیر ربات می‌تواند تنظیمات را تغییر دهد."
            )
            return
        if command in ("/help", "/settings"):
            await self.telegram_to(
                chat_id, self.settings_text(), self.settings_keyboard()
            )
            return
        if command not in ("/high", "/low") or len(parts) != 2:
            await self.telegram_to(chat_id, "Unknown command. Send /settings for help.")
            return

        side = command[1:]
        value = parts[1]
        if value in ("on", "off"):
            setattr(self.preferences, f"{side}_enabled", value == "on")
        else:
            try:
                threshold = float(value)
            except ValueError:
                await self.telegram_to(chat_id, "Value must be on, off, or a number.")
                return
            if not 0 <= threshold <= 100:
                await self.telegram_to(chat_id, "RSI value must be between 0 and 100.")
                return
            setattr(self.preferences, f"{side}_threshold", threshold)
            setattr(self.preferences, f"{side}_enabled", True)
        self.save_preferences()
        await self.telegram_to(
            chat_id, "✅ ذخیره شد\n\n" + self.settings_text(), self.settings_keyboard()
        )

    @staticmethod
    def subscription_keyboard(subscribed: bool) -> dict[str, object]:
        if subscribed:
            button = {"text": "🔕 قطع دریافت هشدار", "callback_data": "sub:off"}
        else:
            button = {"text": "🔔 فعال‌کردن هشدار", "callback_data": "sub:on"}
        return {"inline_keyboard": [[button]]}

    async def answer_callback(self, callback_id: str, text: str = "") -> None:
        url = f"https://api.telegram.org/bot{self.settings.telegram_bot_token}/answerCallbackQuery"
        response = await self.client.post(
            url, json={"callback_query_id": callback_id, "text": text}
        )
        response.raise_for_status()

    async def edit_settings_menu(self, chat_id: str, message_id: int) -> None:
        url = f"https://api.telegram.org/bot{self.settings.telegram_bot_token}/editMessageText"
        response = await self.client.post(
            url,
            json={
                "chat_id": chat_id,
                "message_id": message_id,
                "text": self.settings_text(),
                "reply_markup": self.settings_keyboard(),
            },
        )
        # "message is not modified" is harmless when Refresh is pressed quickly.
        if response.status_code == 400 and "not modified" in response.text.lower():
            return
        response.raise_for_status()

    async def handle_callback(self, callback: dict[str, object]) -> None:
        callback_id = str(callback.get("id", ""))
        message = callback.get("message", {})
        if not isinstance(message, dict):
            return
        chat = message.get("chat", {})
        if not isinstance(chat, dict):
            return
        chat_id = str(chat.get("id", ""))
        message_id = int(message.get("message_id", 0))
        data = str(callback.get("data", ""))
        if data.startswith("sub:"):
            subscribe = data == "sub:on"
            if subscribe and chat_id not in self.preferences.subscribers:
                self.preferences.subscribers.append(chat_id)
            if not subscribe and chat_id in self.preferences.subscribers:
                self.preferences.subscribers.remove(chat_id)
            self.save_preferences()
            await self.answer_callback(
                callback_id,
                "هشدارها فعال شد ✅" if subscribe else "هشدارها متوقف شد 🔕",
            )
            url = f"https://api.telegram.org/bot{self.settings.telegram_bot_token}/editMessageReplyMarkup"
            response = await self.client.post(
                url,
                json={
                    "chat_id": chat_id,
                    "message_id": message_id,
                    "reply_markup": self.subscription_keyboard(subscribe),
                },
            )
            response.raise_for_status()
            return
        if chat_id not in self.settings.admin_chat_ids:
            await self.answer_callback(callback_id, "اجازه تغییر تنظیمات را ندارید.")
            return

        notice = "ذخیره شد ✅"
        if data.startswith("toggle:"):
            side = data.split(":", 1)[1]
            if side in ("high", "low"):
                key = f"{side}_enabled"
                setattr(self.preferences, key, not getattr(self.preferences, key))
        elif data.startswith("adjust:"):
            _, side, amount = data.split(":", 2)
            if side in ("high", "low"):
                key = f"{side}_threshold"
                new_value = min(100.0, max(0.0, getattr(self.preferences, key) + float(amount)))
                setattr(self.preferences, key, new_value)
        elif data.startswith("tf:"):
            interval = data.split(":", 1)[1]
            if interval in self.preferences.intervals:
                if len(self.preferences.intervals) == 1:
                    notice = "حداقل یک تایم‌فریم باید روشن باشد."
                else:
                    self.preferences.intervals.remove(interval)
            else:
                self.preferences.intervals.append(interval)
                order = ("15m", "30m", "1h", "4h", "1d")
                self.preferences.intervals.sort(key=order.index)
        elif data != "refresh":
            notice = "دکمه نامعتبر است."

        self.save_preferences()
        await self.answer_callback(callback_id, notice)
        await self.edit_settings_menu(chat_id, message_id)

    async def telegram_to(
        self, chat_id: str, text: str, reply_markup: dict[str, object] | None = None
    ) -> None:
        url = f"https://api.telegram.org/bot{self.settings.telegram_bot_token}/sendMessage"
        async with self.telegram_lock:
            delay = 1.05 - (time.monotonic() - self.last_telegram_send)
            if delay > 0:
                await asyncio.sleep(delay)
            payload: dict[str, object] = {"chat_id": chat_id, "text": text}
            if reply_markup is not None:
                payload["reply_markup"] = reply_markup
            response = await self.client.post(url, json=payload)
            response.raise_for_status()
            if not response.json().get("ok"):
                raise RuntimeError("Telegram rejected message")
            self.last_telegram_send = time.monotonic()

    async def command_loop(self) -> None:
        url = f"https://api.telegram.org/bot{self.settings.telegram_bot_token}/getUpdates"
        # Confirm and skip historical updates on startup. This prevents an old
        # button click from being applied again after a container restart.
        try:
            response = await self.client.get(
                url,
                params={"offset": -1, "timeout": 0},
                timeout=15,
            )
            response.raise_for_status()
            old_updates = response.json().get("result", [])
            if old_updates:
                self.telegram_offset = int(old_updates[-1]["update_id"]) + 1
                await self.client.get(
                    url,
                    params={"offset": self.telegram_offset, "timeout": 0},
                    timeout=15,
                )
        except Exception as exc:
            LOGGER.warning("Could not clear historical Telegram updates: %s", exc)
        while not self.stop_event.is_set():
            try:
                params: dict[str, object] = {
                    "timeout": 10,
                    "allowed_updates": json.dumps(["message", "callback_query"]),
                }
                if self.telegram_offset is not None:
                    params["offset"] = self.telegram_offset
                response = await self.client.get(url, params=params, timeout=15)
                response.raise_for_status()
                for update in response.json().get("result", []):
                    self.telegram_offset = int(update["update_id"]) + 1
                    callback = update.get("callback_query")
                    if callback:
                        await self.handle_callback(callback)
                        continue
                    message = update.get("message", {})
                    text = message.get("text", "")
                    chat_id = str(message.get("chat", {}).get("id", ""))
                    if text.startswith("/") and chat_id:
                        await self.handle_command(chat_id, text)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOGGER.warning("Telegram command polling failed: %s", exc)
                await asyncio.sleep(5)

    async def check_symbol(self, symbol: str, interval: str, now: float) -> None:
        try:
            rsi, price = await self.get_rsi(symbol, interval)
            zone = self.zone_for(rsi)
            state_key = f"{symbol}:{interval}"
            state = self.states.setdefault(state_key, AlertState())
            cooldown = self.settings.alert_cooldown_minutes * 60
            should_alert = zone != "normal" and (
                zone != state.zone
                or (cooldown > 0 and now - state.last_alert_at >= cooldown)
            )
            if should_alert:
                direction = "LOW 📉" if zone == "low" else "HIGH 📈"
                await self.telegram(
                    f"RSI ALERT — {direction}\n"
                    f"Contract: {self.display_symbol(symbol)}\n"
                    f"RSI({self.settings.rsi_period}): {rsi:.2f}\n"
                    f"Price: {price:g} {self.settings.quote_asset}\n"
                    f"Timeframe: {interval}\n"
                    f"Candle: completed"
                )
                state.last_alert_at = now
                LOGGER.info("Alert sent for %s %s: RSI %.2f", symbol, interval, rsi)
            state.zone = zone
        except Exception as exc:  # Keep one bad/delisted pair from stopping the scan.
            LOGGER.warning("Could not check %s %s: %s", symbol, interval, exc)

    async def scan(self) -> None:
        symbols = await self.symbols()
        intervals = tuple(self.preferences.intervals)
        LOGGER.info(
            "Scanning %d Toobit %s symbols on timeframes: %s",
            len(symbols),
            self.settings.market_type,
            ", ".join(intervals),
        )
        now = time.time()
        await asyncio.gather(
            *(
                self.check_symbol(symbol, interval, now)
                for symbol in symbols
                for interval in intervals
            )
        )

    async def run(self) -> None:
        await self.telegram("✅ RSI monitor started")
        command_task = asyncio.create_task(self.command_loop())
        try:
            while not self.stop_event.is_set():
                started = time.monotonic()
                try:
                    await self.scan()
                except Exception:
                    LOGGER.exception("Scan failed")
                remaining = max(
                    0.0,
                    self.settings.scan_every_seconds - (time.monotonic() - started),
                )
                try:
                    await asyncio.wait_for(self.stop_event.wait(), timeout=remaining)
                except TimeoutError:
                    pass
        finally:
            command_task.cancel()
            await asyncio.gather(command_task, return_exceptions=True)


async def async_main() -> None:
    settings = Settings.from_env()
    service = RSIAlertService(settings)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, service.stop_event.set)
        except NotImplementedError:  # Windows uses KeyboardInterrupt instead.
            pass
    try:
        await service.run()
    finally:
        await service.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # httpx logs complete request URLs; Telegram URLs contain the bot token.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        LOGGER.info("Stopped")
