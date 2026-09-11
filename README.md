# Cryptocurrency RSI Telegram alerts

An always-on Python service that checks Toobit USDT-M Futures contracts and sends a
Telegram message when a completed candle's Wilder RSI is below 30 or above 90.
It does not trade and does not need a Toobit account or API key.

## 1. Create the Telegram notification bot

1. In Telegram, open **@BotFather**, send `/newbot`, and copy the bot token.
2. Open the new bot and press **Start** (or send it any message).
3. In a browser, open `https://api.telegram.org/bot<TOKEN>/getUpdates`.
4. Find `message.chat.id` in the JSON. This is the chat ID. A private chat ID is
   usually a positive number; group IDs are usually negative.

Never commit or share the token. Anyone with it can control the bot.

## 2. Configure it

```bash
cp .env.example .env
```

Edit `.env` and set `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`.
To notify several people, separate their chat IDs with commas.

The authorized Telegram administrator can send `/settings` to open a Persian
button menu. The menu enables/disables High and Low alerts, adjusts their values,
selects one or more candle timeframes, and shows/hides Toobit RWA/TradFi futures
(stocks, forex, and metals). Changes persist across restarts.
Any Telegram user who presses **Start** is automatically subscribed to alerts and
can later stop or resume them with the friendly notification button. Only the
configured administrator can change RSI settings.

`SYMBOLS=ALL` monitors every active crypto-only linear Toobit Futures contract
quoted in USDT. RWA/TradFi contracts such as stocks, forex, and metals are excluded.
Set `MARKET_TYPE=spot` to use Spot instead. To monitor
only selected coins, use `SYMBOLS=BTC,ETH,SOL`. The program adds `USDT` when needed.
The default intervals are 30 minutes and one day. Toobit intervals such as `15m`,
`1h`, and `4h` also work. Use commas to monitor several, such as `30m,1d`.

An alert is sent when a symbol enters a low/high zone. If it stays there, the alert
is repeated after `ALERT_COOLDOWN_MINUTES`. It can alert again immediately after it
returns to normal and later crosses a threshold again.

## 3. Run on your computer

Python 3.11 or newer is recommended.

```bash
python -m venv .venv
# Linux/macOS: source .venv/bin/activate
# Windows PowerShell: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m rsi_alert
```

Leave the terminal open. Stop it with Ctrl+C.

## 4. Run continuously on a server (recommended)

Any small Ubuntu VPS with Docker is enough; this service has no inbound ports and
does not need a domain. Install Docker, copy this project to the server, create the
`.env` file, then run:

```bash
docker compose up -d --build
docker compose logs -f
```

It restarts automatically after a crash or server reboot. Update it with:

```bash
docker compose up -d --build
```

Without Docker, place the project in `/opt/rsi-alert`, install its virtual
environment, create a dedicated `rsi-alert` system user, copy
`deploy/rsi-alert.service` to `/etc/systemd/system/`, then run:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now rsi-alert
sudo journalctl -u rsi-alert -f
```

## Test

```bash
pip install -r requirements-dev.txt
pytest
```

RSI is an indicator, not a guarantee of a price reversal or financial advice.
