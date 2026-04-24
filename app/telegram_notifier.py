from __future__ import annotations

import html
from typing import Any

import httpx

from app.models import AlertEvent, AlertPriority


class TelegramNotificationError(RuntimeError):
    pass


class TelegramNotifier:
    def __init__(
        self,
        bot_token: str,
        chat_id: str,
        *,
        parse_mode: str = "HTML",
        timeout_seconds: float = 10.0,
    ) -> None:
        if not bot_token:
            raise TelegramNotificationError("TELEGRAM_BOT_TOKEN no configurado.")
        if not chat_id:
            raise TelegramNotificationError("TELEGRAM_CHAT_ID no configurado.")

        self._bot_token = bot_token
        self._chat_id = chat_id
        self._parse_mode = parse_mode
        self._client = httpx.AsyncClient(
            base_url=f"https://api.telegram.org/bot{bot_token}",
            timeout=httpx.Timeout(timeout_seconds),
        )

    async def close(self) -> None:
        await self._client.aclose()

    @staticmethod
    def _format_number(value: float | None, digits: int = 6) -> str:
        if value is None:
            return "N/D"
        return f"{value:.{digits}f}"

    def _build_message(self, event: AlertEvent) -> str:
        lines = [f"<b>{html.escape(event.priority.value)} | Trading Alert</b>"]

        if event.symbol != "SYSTEM":
            lines.extend(
                [
                    f"<b>Símbolo:</b> {html.escape(event.symbol)}",
                    f"<b>Side:</b> {html.escape(event.side.value if event.side else 'N/D')}",
                    f"<b>Precio actual:</b> {self._format_number(event.current_price)}",
                    f"<b>Entrada:</b> {self._format_number(event.entry)}",
                    f"<b>SL:</b> {self._format_number(event.stop_loss)}",
                ]
            )
            if event.take_profits:
                lines.append(
                    "<b>TPs:</b> "
                    + " / ".join(self._format_number(price) for price in event.take_profits)
                )
            pnl_parts: list[str] = []
            if event.approx_pnl_usdt is not None:
                pnl_parts.append(f"{event.approx_pnl_usdt:+.4f} USDT")
            if event.approx_pnl_pct is not None:
                pnl_parts.append(f"{event.approx_pnl_pct:+.2f}%")
            lines.append(f"<b>PnL aprox:</b> {' | '.join(pnl_parts) if pnl_parts else 'N/D'}")

            if event.bias is not None or event.ema_fast is not None or event.ema_slow is not None:
                lines.append(
                    "<b>Bias:</b> "
                    f"{html.escape(event.bias.value if event.bias else 'neutral')} | "
                    f"EMAf {self._format_number(event.ema_fast, 4)} | "
                    f"EMAs {self._format_number(event.ema_slow, 4)} | "
                    f"Struct {html.escape(event.structure or 'neutral')}"
                )

        lines.append(f"<b>Motivo:</b> {html.escape(event.reason)}")
        if event.note:
            lines.append(f"<b>Nota:</b> {html.escape(event.note)}")
        return "\n".join(lines)

    async def send_alert(self, event: AlertEvent) -> dict[str, Any]:
        payload = {
            "chat_id": self._chat_id,
            "text": self._build_message(event),
            "parse_mode": self._parse_mode,
            "disable_notification": event.priority == AlertPriority.INFO,
        }
        try:
            response = await self._client.post("/sendMessage", json=payload)
        except httpx.HTTPError as exc:
            raise TelegramNotificationError(f"Error HTTP enviando a Telegram: {exc}") from exc

        data = response.json()
        if response.status_code >= 400 or not data.get("ok", False):
            raise TelegramNotificationError(
                data.get("description", f"Telegram respondió con {response.status_code}")
            )
        return data
