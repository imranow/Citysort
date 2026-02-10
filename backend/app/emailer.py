"""SMTP email delivery helper."""
from __future__ import annotations

import smtplib
from email.message import EmailMessage
from email.utils import formataddr

from .config import (
    EMAIL_ENABLED,
    EMAIL_FROM_ADDRESS,
    EMAIL_FROM_NAME,
    SMTP_HOST,
    SMTP_PASSWORD,
    SMTP_PORT,
    SMTP_TIMEOUT_SECONDS,
    SMTP_USERNAME,
    SMTP_USE_SSL,
    SMTP_USE_TLS,
)


def email_configured() -> bool:
    if not EMAIL_ENABLED:
        return False
    return bool(SMTP_HOST and EMAIL_FROM_ADDRESS)


def send_email(*, to_email: str, subject: str, body: str) -> None:
    if not email_configured():
        raise RuntimeError("Email sending is not configured.")

    msg = EmailMessage()
    msg["From"] = formataddr((EMAIL_FROM_NAME, EMAIL_FROM_ADDRESS))
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.set_content(body)

    if SMTP_USE_SSL:
        with smtplib.SMTP_SSL(host=SMTP_HOST, port=SMTP_PORT, timeout=SMTP_TIMEOUT_SECONDS) as server:
            if SMTP_USERNAME:
                server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.send_message(msg)
        return

    with smtplib.SMTP(host=SMTP_HOST, port=SMTP_PORT, timeout=SMTP_TIMEOUT_SECONDS) as server:
        if SMTP_USE_TLS:
            server.starttls()
        if SMTP_USERNAME:
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
        server.send_message(msg)
